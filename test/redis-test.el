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
                :type 'redis-error)
  (should-error (redis-parse-response "*1\r\n-ERR nested\r\n")
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

(ert-deftest redis-test-malformed-numbers-signal-protocol-error ()
  "RESP integers and lengths should use strict decimal syntax."
  (dolist (response '(":wat\r\n" "$wat\r\n\r\n" "*wat\r\n"))
    (should-error (redis-parse-response response)
                  :type 'redis-protocol-error)))

(ert-deftest redis-test-timeout-invalidates-connection ()
  "A response timeout should close the connection before it can be reused."
  (let* ((redis-response-timeout 0)
         (buffer (generate-new-buffer " *redis-test-timeout*"))
         (process (make-pipe-process :name "redis-test-timeout"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process)))
    (cl-letf (((symbol-function 'process-send-string) #'ignore))
      (should-error (redis-command conn "PING")
                    :type 'redis-timeout-error))
    (should (redis-conn-closed conn))
    (should-not (redis-live-p conn))
    (should-not (buffer-live-p buffer))))

(ert-deftest redis-test-protocol-error-invalidates-connection ()
  "A malformed server response should close the connection."
  (let* ((buffer (generate-new-buffer " *redis-test-protocol-error*"))
         (process (make-pipe-process :name "redis-test-protocol-error"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process)))
    (with-current-buffer buffer
      (set-buffer-multibyte nil)
      (insert "?bad\r\n"))
    (cl-letf (((symbol-function 'process-send-string) #'ignore))
      (should-error (redis-command conn "PING")
                    :type 'redis-protocol-error))
    (should (redis-conn-closed conn))
    (should-not (buffer-live-p buffer))))

(ert-deftest redis-test-server-error-keeps-connection-live ()
  "A consumed Redis error reply should not invalidate the connection."
  (let* ((buffer (generate-new-buffer " *redis-test-server-error*"))
         (process (make-pipe-process :name "redis-test-server-error"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process)))
    (unwind-protect
        (progn
          (with-current-buffer buffer
            (set-buffer-multibyte nil)
            (insert "-ERR bad\r\n"))
          (cl-letf (((symbol-function 'process-send-string) #'ignore))
            (should-error (redis-command conn "PING") :type 'redis-error))
          (should (redis-live-p conn))
          (should-not (redis-conn-closed conn)))
      (redis-disconnect conn))))

(ert-deftest redis-test-local-encoding-error-keeps-connection-live ()
  "Invalid local command arguments should not invalidate the connection."
  (let* ((buffer (generate-new-buffer " *redis-test-encode-error*"))
         (process (make-pipe-process :name "redis-test-encode-error"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process)))
    (unwind-protect
        (progn
          (should-error (redis-command conn "SET" '(unsupported))
                        :type 'redis-protocol-error)
          (should (redis-live-p conn))
          (should-not (redis-conn-closed conn)))
      (redis-disconnect conn))))

(ert-deftest redis-test-fragmented-response-scans-incrementally ()
  "Fragmented input should resume from the last complete RESP token."
  (let* ((buffer (generate-new-buffer " *redis-test-fragmented*"))
         (process (make-pipe-process :name "redis-test-fragmented"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process))
         (chunks (list "bar\r\n"))
         scan-states)
    (unwind-protect
        (progn
          (with-current-buffer buffer
            (set-buffer-multibyte nil)
            (insert "*2\r\n$3\r\nfoo\r\n$3\r\n"))
          (process-put process 'redis-response-start
                       (with-current-buffer buffer (point-min)))
          (let ((original-scan (symbol-function 'redis--scan-available)))
            (cl-letf (((symbol-function 'redis--scan-available)
                       (lambda (state)
                         (push state scan-states)
                         (funcall original-scan state)))
                      ((symbol-function 'accept-process-output)
                       (lambda (&rest _)
                         (with-current-buffer buffer
                           (goto-char (point-max))
                           (insert (pop chunks)))
                         t)))
              (should (equal (redis--read-response conn) '("foo" "bar")))))
          (should-not chunks)
          (should (> (length scan-states) 1))
          (let ((state (car scan-states)))
            (dolist (seen scan-states)
              (should (eq seen state))))
          (with-current-buffer buffer
            (should (= (buffer-size) 0))))
      (redis-disconnect conn))))

(ert-deftest redis-test-incremental-line-scan-resumes-near-tail ()
  "Incomplete RESP lines should not be rescanned from their prefix."
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (insert "+partial")
    (let ((state (redis--make-scan-state :pos (point-min))))
      (should-not (redis--scan-available state))
      (should (> (redis--scan-state-line-search state) (point-min)))
      (goto-char (point-max))
      (insert " response\r\n")
      (should (= (redis--scan-available state) (point-max))))))

(ert-deftest redis-test-quit-after-send-invalidates-connection ()
  "A quit after command send must discard the ambiguous stream."
  (let* ((buffer (generate-new-buffer " *redis-test-quit*"))
         (process (make-pipe-process :name "redis-test-quit"
                                     :buffer buffer :noquery t))
         (conn (make-redis-conn :process process))
         quit-seen)
    (cl-letf (((symbol-function 'process-send-string) #'ignore)
              ((symbol-function 'redis--read-response)
               (lambda (_conn) (signal 'quit nil))))
      (condition-case nil
          (redis-command conn "PING")
        (quit (setq quit-seen t))))
    (should quit-seen)
    (should (redis-conn-closed conn))
    (should-not (redis-live-p conn))
    (should-not (buffer-live-p buffer))))

(ert-deftest redis-test-response-resource-limits ()
  "RESP byte, bulk, element, and nesting limits should fail closed."
  (let ((redis-max-response-bytes 4))
    (should-error (redis-parse-response "+OK\r\n")
                  :type 'redis-protocol-error))
  (let ((redis-max-bulk-bytes 2))
    (should-error (redis-parse-response "$3\r\nfoo\r\n")
                  :type 'redis-protocol-error))
  (let ((redis-max-elements 1))
    (should-error (redis-parse-response "*2\r\n:1\r\n:2\r\n")
                  :type 'redis-protocol-error))
  (let ((redis-max-depth 1))
    (should-error (redis-parse-response "*1\r\n*1\r\n+OK\r\n")
                  :type 'redis-protocol-error))
  (should (= (car (redis-parse-response ":9223372036854775807\r\n"))
             9223372036854775807))
  (should (= (car (redis-parse-response ":-9223372036854775808\r\n"))
             -9223372036854775808))
  (dolist (response '(":9223372036854775808\r\n"
                      ":-9223372036854775809\r\n"
                      ":000000000000000000001\r\n"
                      "$000000000000000000001\r\nx\r\n"))
    (should-error (redis-parse-response response)
                  :type 'redis-protocol-error)))

(ert-deftest redis-test-public-response-limit-allows-trailing-frame ()
  "The public byte limit should apply to the first consumed response only."
  (let ((redis-max-response-bytes 5))
    (should (equal (redis-parse-response "+OK\r\n+NEXT\r\n")
                   '("OK" . 5)))
    (should-error (redis-parse-response "+HEY\r\n")
                  :type 'redis-protocol-error)))

(ert-deftest redis-test-element-budget-resets-between-public-parses ()
  "Each public parse should receive an independent element budget."
  (let ((redis-max-elements 1))
    (should (equal (car (redis-parse-response "*1\r\n+OK\r\n"))
                   '("OK")))
    (should (equal (car (redis-parse-response "*1\r\n+OK\r\n"))
                   '("OK")))))

(ert-deftest redis-test-connect-is-bounded-and-cleans-up ()
  "Connection setup should be asynchronous, bounded, and leak-free."
  (let (created-process created-buffer nowait)
    (cl-letf (((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq nowait (plist-get args :nowait)
                       created-buffer (plist-get args :buffer)
                       created-process
                       (make-pipe-process :name "redis-test-connect-timeout"
                                          :buffer created-buffer :noquery t))))
              ((symbol-function 'redis--wait-for-connect)
               (lambda (_process)
                 (signal 'redis-connection-error '("connect timeout")))))
      (should-error (redis-connect '(:host "db" :port 6379))
                    :type 'redis-connection-error))
    (should nowait)
    (should-not (process-live-p created-process))
    (should-not (buffer-live-p created-buffer))))

(ert-deftest redis-test-connect-wait-dispatches-global-process-events ()
  "Asynchronous connect waits should not restrict event dispatch to the socket."
  (let ((status 'connect)
        accepted-process)
    (cl-letf (((symbol-function 'process-status) (lambda (_process) status))
              ((symbol-function 'redis--process-live-p)
               (lambda (_process) (eq status 'open)))
              ((symbol-function 'accept-process-output)
               (lambda (process &rest _)
                 (setq accepted-process process
                       status 'open)
                 t)))
      (redis--wait-for-connect :process))
    (should-not accepted-process)))

(ert-deftest redis-test-connect-quit-cleans-up ()
  "Quitting during connection setup should not leak transport resources."
  (let (created-process created-buffer quit-seen)
    (cl-letf (((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq created-buffer (plist-get args :buffer)
                       created-process
                       (make-pipe-process :name "redis-test-connect-quit"
                                          :buffer created-buffer :noquery t))))
              ((symbol-function 'redis--wait-for-connect)
               (lambda (_process) (signal 'quit nil))))
      (condition-case nil
          (redis-connect '(:host "db" :port 6379))
        (quit (setq quit-seen t))))
    (should quit-seen)
    (should-not (process-live-p created-process))
    (should-not (buffer-live-p created-buffer))))

(ert-deftest redis-test-process-sentinel-does-not-pollute-wire-buffer ()
  "A remote close should stay transport state, not become RESP payload."
  (let* ((buffer (generate-new-buffer " *redis-test-sentinel*"))
         (process (make-pipe-process :name "redis-test-sentinel"
                                     :buffer buffer :noquery t
                                     :sentinel #'redis--process-sentinel))
         (conn (make-redis-conn :process process)))
    (unwind-protect
        (progn
          (delete-process process)
          (accept-process-output nil 0.01)
          (with-current-buffer buffer
            (should (= (buffer-size) 0)))
          (should (process-get process 'redis-error))
          (should-error (redis--read-response conn)
                        :type 'redis-connection-error))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

(ert-deftest redis-test-connect-authenticates-and-selects-database ()
  "Connection setup should issue AUTH before SELECT."
  (let (calls conn creation-sentinel-present installed-sentinels)
    (cl-letf (((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq creation-sentinel-present (plist-member args :sentinel))
                 (make-pipe-process :name "redis-test-connect"
                                    :buffer (plist-get args :buffer)
                                    :noquery t)))
              ((symbol-function 'redis-command)
               (lambda (redis-connection command &rest arguments)
                 (push (process-sentinel
                        (redis-conn-process redis-connection))
                       installed-sentinels)
                 (push (cons command arguments) calls)
                 "OK")))
      (unwind-protect
          (setq conn (redis-connect '(:host "db" :port 6379
                                      :user "app" :password "secret"
                                      :database 2)))
        (when conn (redis-disconnect conn))))
    (should-not creation-sentinel-present)
    (should (equal installed-sentinels
                   '(redis--process-sentinel redis--process-sentinel)))
    (should (equal (nreverse calls)
                   '(("AUTH" "app" "secret") ("SELECT" 2))))))

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
