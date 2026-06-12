;;; redis-test.el --- Tests for redis.el -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Code:

(require 'ert)
(require 'redis)

(ert-deftest redis-test-encode-command-uses-resp-bulk-strings ()
  "Command encoding should produce RESP2 bulk-array bytes."
  (should (equal (redis-encode-command "SET" "name" "lucius")
                 "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nlucius\r\n")))

(ert-deftest redis-test-encode-command-counts-utf-8-bytes ()
  "Command encoding should count bytes, not characters."
  (should (equal (redis-encode-command "SET" "city" "上海")
                 (encode-coding-string
                  "*3\r\n$3\r\nSET\r\n$4\r\ncity\r\n$6\r\n上海\r\n"
                  'utf-8 t))))

(ert-deftest redis-test-parse-basic-response-types ()
  "RESP parser should decode simple, integer, bulk, null, and array values."
  (should (equal (redis-parse-response "+OK\r\n") '("OK" . 5)))
  (should (equal (redis-parse-response ":42\r\n") '(42 . 5)))
  (should (equal (redis-parse-response "$5\r\nhello\r\n") '("hello" . 11)))
  (should (equal (redis-parse-response "$-1\r\n") '(nil . 5)))
  (should (equal (redis-parse-response "*2\r\n$3\r\nfoo\r\n:7\r\n")
                 '(("foo" 7) . 17))))

(ert-deftest redis-test-error-response-signals-redis-error ()
  "RESP error replies should not be ordinary values."
  (should-error (redis-parse-response "-WRONGTYPE bad type\r\n")
                :type 'redis-error))

(ert-deftest redis-test-read-response-consumes-error-before-signaling ()
  "Connection reads should advance past Redis error replies."
  (let* ((buffer (generate-new-buffer " *redis-test*"))
         (process nil)
         (conn nil))
    (unwind-protect
        (progn
          (with-current-buffer buffer
            (set-buffer-multibyte nil)
            (insert "-ERR bad\r\n+OK\r\n"))
          (setq process (make-pipe-process
                         :name "redis-test"
                         :buffer buffer
                         :noquery t))
          (process-put process 'redis-response-start
                       (with-current-buffer buffer (point-min)))
          (setq conn (make-redis-conn :process process))
          (should-error (redis--read-response conn) :type 'redis-error)
          (should (equal (redis--read-response conn) "OK"))
          (with-current-buffer buffer
            (should (= (buffer-size) 0))))
      (when (processp process)
        (delete-process process))
      (when (buffer-live-p buffer)
        (kill-buffer buffer)))))

(ert-deftest redis-test-incomplete-response-signals-protocol-error ()
  "Public parsing should reject incomplete responses."
  (should-error (redis-parse-response "$5\r\nhel")
                :type 'redis-protocol-error))

(ert-deftest redis-test-live-basic-commands ()
  "Basic command path should work against a live Redis server."
  :tags '(:redis-live)
  (skip-unless (equal (getenv "REDIS_TEST_LIVE") "1"))
  (let* ((host (or (getenv "REDIS_TEST_HOST") "127.0.0.1"))
         (port (string-to-number (or (getenv "REDIS_TEST_PORT") "6379")))
         (conn (redis-connect (list :host host :port port))))
    (unwind-protect
        (progn
          (should (redis-live-p conn))
          (should (equal (redis-command conn "PING") "PONG"))
          (should (equal (redis-command conn "SET" "redis-el:string" "hello") "OK"))
          (should (equal (redis-decode-string
                          (redis-command conn "GET" "redis-el:string"))
                         "hello"))
          (should (= (redis-command conn "HSET" "redis-el:hash" "field" "value") 1))
          (should (equal (mapcar #'redis-decode-string
                                  (redis-command conn "HGETALL" "redis-el:hash"))
                         '("field" "value")))
          (should (consp (redis-command conn "SCAN" 0 "MATCH" "redis-el:*"))))
      (redis-disconnect conn))))

(provide 'redis-test)
;;; redis-test.el ends here
