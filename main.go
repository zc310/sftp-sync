package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"
	"go.etcd.io/bbolt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var hashesBucket = []byte("file_hashes")

const workerCount = 3

type syncTask struct {
	local  string
	remote string
	rel    string
	dir    bool
}

type Config struct {
	LocalDir      string   `json:"local_dir"`
	RemoteDir     string   `json:"remote_dir"`
	Database      string   `json:"database"`
	Address       string   `json:"address"`
	Username      string   `json:"username"`
	Password      string   `json:"password,omitempty"`
	PrivateKey    string   `json:"private_key,omitempty"`
	KeyPassphrase string   `json:"key_passphrase,omitempty"`
	HostKeyFile   string   `json:"host_key_file"`
	Ignore        []string `json:"ignore,omitempty"`
}

func main() {
	configPath := flag.String("config", "sftp-sync.json", "配置文件路径")
	flag.Parse()

	if err := run(*configPath); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("读取配置: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("解析配置: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	root, err := filepath.Abs(cfg.LocalDir)
	if err != nil {
		return fmt.Errorf("解析本地目录: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("访问本地目录: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("本地路径不是目录: %s", root)
	}

	key, err := os.ReadFile(cfg.PrivateKey)
	if err != nil && cfg.PrivateKey != "" {
		return fmt.Errorf("读取 SSH 私钥: %w", err)
	}
	clientConfig := &ssh.ClientConfig{User: cfg.Username, Timeout: 15 * time.Second}
	if len(key) > 0 {
		var signer ssh.Signer
		if cfg.KeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(cfg.KeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(key)
		}
		if err != nil {
			return fmt.Errorf("解析 SSH 私钥: %w", err)
		}
		clientConfig.Auth = append(clientConfig.Auth, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		clientConfig.Auth = append(clientConfig.Auth, ssh.Password(cfg.Password))
	}
	if len(clientConfig.Auth) == 0 {
		return errors.New("必须配置 password 或 private_key")
	}
	if cfg.HostKeyFile == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("获取用户主目录: %w", err)
		}
		cfg.HostKeyFile = filepath.Join(home, ".ssh", "known_hosts")
	}
	clientConfig.HostKeyCallback, err = knownhosts.New(cfg.HostKeyFile)
	if err != nil {
		return fmt.Errorf("读取 host_key_file: %w", err)
	}

	dbPath, err := filepath.Abs(cfg.Database)
	if err != nil {
		return fmt.Errorf("解析数据库路径: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return fmt.Errorf("创建数据库目录: %w", err)
	}
	db, err := bbolt.Open(dbPath, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("打开哈希数据库: %w", err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(hashesBucket)
		return err
	}); err != nil {
		return fmt.Errorf("初始化哈希数据库: %w", err)
	}

	var tasks []syncTask
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if ignored(rel, cfg.Ignore) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		remotePath := filepath.Join(cfg.RemoteDir, filepath.FromSlash(rel))
		if entry.IsDir() {
			tasks = append(tasks, syncTask{local: path, remote: remotePath, dir: true})
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		tasks = append(tasks, syncTask{local: path, remote: remotePath, rel: rel})
		return nil
	})
	if err != nil {
		return fmt.Errorf("遍历或同步目录: %w", err)
	}

	clients := make([]*sftp.Client, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		conn, err := ssh.Dial("tcp", cfg.Address, clientConfig)
		if err != nil {
			for _, client := range clients {
				client.Close()
			}
			return fmt.Errorf("建立第 %d 个 SSH 连接: %w", i+1, err)
		}
		client, err := sftp.NewClient(conn)
		if err != nil {
			conn.Close()
			for _, client := range clients {
				client.Close()
			}
			return fmt.Errorf("建立第 %d 个 SFTP 会话: %w", i+1, err)
		}
		clients = append(clients, client)
	}
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()
	if err := clients[0].MkdirAll(cfg.RemoteDir); err != nil {
		return fmt.Errorf("创建远端目录: %w", err)
	}

	jobs := make(chan syncTask)
	var uploaded, skipped atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, len(tasks))
	for _, client := range clients {
		wg.Add(1)
		go func(client *sftp.Client) {
			defer wg.Done()
			for task := range jobs {
				if task.dir {
					if err := client.MkdirAll(task.remote); err != nil {
						errs <- fmt.Errorf("创建远端目录 %s: %w", task.remote, err)
					}
					continue
				}
				didUpload, err := syncFile(client, db, task)
				if err != nil {
					errs <- err
					continue
				}
				if didUpload {
					uploaded.Add(1)
				} else {
					skipped.Add(1)
				}
			}
		}(client)
	}
	for _, task := range tasks {
		jobs <- task
	}
	close(jobs)
	wg.Wait()
	close(errs)
	var failures []error
	for err := range errs {
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	log.Printf("同步完成: 上传 %d 个，跳过 %d 个", uploaded.Load(), skipped.Load())
	return nil
}

func (c Config) validate() error {
	if c.LocalDir == "" || c.RemoteDir == "" || c.Database == "" || c.Address == "" || c.Username == "" {
		return errors.New("local_dir、remote_dir、database、address、username 均为必填项")
	}
	return nil
}

func uploadFile(client *sftp.Client, localPath, remotePath string) error {
	if err := client.MkdirAll(filepath.ToSlash(filepath.Dir(remotePath))); err != nil {
		return err
	}
	src, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := client.Create(remotePath)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func syncFile(client *sftp.Client, db *bbolt.DB, task syncTask) (bool, error) {
	digest, err := fileHash(task.local)
	if err != nil {
		return false, fmt.Errorf("计算 %s 哈希: %w", task.rel, err)
	}
	var oldHash []byte
	if err := db.View(func(tx *bbolt.Tx) error {
		oldHash = append([]byte(nil), tx.Bucket(hashesBucket).Get([]byte(task.rel))...)
		return nil
	}); err != nil {
		return false, fmt.Errorf("读取 %s 哈希: %w", task.rel, err)
	}
	if string(oldHash) == string(digest) {
		return false, nil
	}
	if err := uploadFile(client, task.local, task.remote); err != nil {
		return false, fmt.Errorf("上传 %s: %w", task.rel, err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(hashesBucket).Put([]byte(task.rel), digest)
	}); err != nil {
		return false, fmt.Errorf("保存 %s 的哈希: %w", task.rel, err)
	}
	log.Printf("已同步 %s (sha256:%s)", task.rel, hex.EncodeToString(digest))
	return true, nil
}

func fileHash(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	closeErr := f.Close()
	if copyErr != nil {
		return nil, copyErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return h.Sum(nil), nil
}

func ignored(rel string, patterns []string) bool {
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if matched, _ := filepath.Match(pattern, rel); matched {
			return true
		}
		if strings.HasSuffix(pattern, "/**") && strings.HasPrefix(rel, strings.TrimSuffix(pattern, "**")) {
			return true
		}
	}
	return false
}
