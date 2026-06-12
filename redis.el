;;; redis.el --- Native Redis RESP client -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Assisted-by: OpenAI Codex:gpt-5.5
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: data, redis, tools
;; URL: https://github.com/LuciusChen/redis.el

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Minimal native Redis client for Emacs Lisp.
;;
;; This package intentionally exposes a small protocol surface: RESP2 over one
;; TCP connection, one command/response at a time, AUTH, SELECT, and structured
;; Redis errors.  Bulk strings are returned as unibyte byte strings; callers
;; should decode them for display with `redis-decode-string'.

;;; Code:

(require 'cl-lib)
(require 'subr-x)

;;;; Errors

(define-error 'redis-error "Redis error")
(define-error 'redis-protocol-error "Redis protocol error" 'redis-error)
(define-error 'redis-timeout-error "Redis response timeout" 'redis-error)
(define-error 'redis-connection-error "Redis connection error" 'redis-error)

;;;; Customization

(defgroup redis nil
  "Native Redis RESP client."
  :group 'data)

(defcustom redis-response-timeout 3
  "Seconds to wait for one Redis response."
  :type 'number
  :group 'redis)

(defcustom redis-default-host "127.0.0.1"
  "Default Redis host used by `redis-connect'."
  :type 'string
  :group 'redis)

(defcustom redis-default-port 6379
  "Default Redis port used by `redis-connect'."
  :type 'integer
  :group 'redis)

;;;; Connection state

(cl-defstruct redis-conn
  "A Redis connection created by `redis-connect'."
  process
  host
  port
  database
  username
  closed
  busy)

(defun redis--buffer-name (host port)
  "Return an internal process buffer name for HOST and PORT."
  (generate-new-buffer-name (format " *redis %s:%s*" host port)))

(defun redis--make-buffer (host port)
  "Return a unibyte process buffer for HOST and PORT."
  (let ((buffer (generate-new-buffer (redis--buffer-name host port))))
    (with-current-buffer buffer
      (set-buffer-multibyte nil))
    buffer))

(defun redis--process-live-p (process)
  "Return non-nil when PROCESS is a live Redis network process."
  (and (processp process)
       (memq (process-status process) '(open run))))

(defun redis-live-p (conn)
  "Return non-nil when CONN is connected."
  (and (redis-conn-p conn)
       (not (redis-conn-closed conn))
       (redis--process-live-p (redis-conn-process conn))))

(defun redis--ensure-live (conn)
  "Signal if CONN is not connected."
  (unless (redis-live-p conn)
    (signal 'redis-connection-error (list "Redis connection closed"))))

;;;; RESP encoding

(defun redis--ascii-bytes (text)
  "Return ASCII TEXT as a unibyte string."
  (encode-coding-string text 'ascii t))

(defun redis--argument-bytes (argument)
  "Return ARGUMENT encoded as Redis bulk bytes."
  (cond
   ((stringp argument)
    (if (multibyte-string-p argument)
        (encode-coding-string argument 'utf-8 t)
      argument))
   ((numberp argument)
    (redis--ascii-bytes (number-to-string argument)))
   ((symbolp argument)
    (redis--ascii-bytes (symbol-name argument)))
   (t
    (signal 'redis-protocol-error
            (list (format "Unsupported Redis command argument: %S" argument))))))

(defun redis-encode-command (command &rest arguments)
  "Return a RESP2 byte string for COMMAND and ARGUMENTS."
  (let* ((parts (mapcar #'redis--argument-bytes (cons command arguments)))
         (header (redis--ascii-bytes (format "*%d\r\n" (length parts)))))
    (apply #'concat
           header
           (cl-loop for part in parts
                    append (list
                            (redis--ascii-bytes
                             (format "$%d\r\n" (length part)))
                            part
                            (redis--ascii-bytes "\r\n"))))))

;;;; RESP parsing

(defconst redis--incomplete (make-symbol "redis-incomplete")
  "Internal marker for incomplete RESP data.")

(defconst redis--error-reply (make-symbol "redis-error-reply")
  "Internal marker for Redis error replies.")

(defun redis--error-reply-p (value)
  "Return non-nil when VALUE is an internal Redis error reply."
  (and (consp value)
       (eq (car value) redis--error-reply)))

(defun redis--decode-line (bytes)
  "Decode RESP line BYTES as UTF-8 text."
  (decode-coding-string bytes 'utf-8 t))

(defun redis-decode-string (bytes &optional coding)
  "Decode Redis bulk BYTES using CODING, defaulting to UTF-8.
If BYTES is nil, return nil."
  (when bytes
    (decode-coding-string bytes (or coding 'utf-8) t)))

(defun redis--crlf-position (bytes start)
  "Return the CRLF position in BYTES at or after START."
  (string-match-p "\r\n" bytes start))

(defun redis--parse-line-payload (bytes start)
  "Return (PAYLOAD . NEXT) for a RESP line in BYTES from START."
  (if-let* ((line-end (redis--crlf-position bytes start)))
      (cons (substring bytes (1+ start) line-end)
            (+ line-end 2))
    redis--incomplete))

(defun redis--parse-number-line (bytes start)
  "Return (NUMBER . NEXT) for a RESP integer-like line in BYTES from START."
  (let ((line (redis--parse-line-payload bytes start)))
    (if (eq line redis--incomplete)
        redis--incomplete
      (cons (string-to-number (car line)) (cdr line)))))

(defun redis--parse-simple-string (bytes start)
  "Return (VALUE . NEXT) for a RESP simple string in BYTES from START."
  (let ((line (redis--parse-line-payload bytes start)))
    (if (eq line redis--incomplete)
        redis--incomplete
      (cons (redis--decode-line (car line)) (cdr line)))))

(defun redis--parse-error (bytes start)
  "Return an internal Redis error reply parsed from BYTES at START."
  (let ((line (redis--parse-line-payload bytes start)))
    (if (eq line redis--incomplete)
        redis--incomplete
      (cons (cons redis--error-reply
                  (redis--decode-line (car line)))
            (cdr line)))))

(defun redis--parse-bulk-string (bytes start)
  "Return (VALUE . NEXT) for a RESP bulk string in BYTES from START."
  (let ((length-line (redis--parse-number-line bytes start)))
    (if (eq length-line redis--incomplete)
        redis--incomplete
      (pcase-let* ((`(,size . ,body-start) length-line)
                   (body-end (+ body-start size))
                   (message-end (+ body-end 2)))
        (cond
         ((= size -1) (cons nil body-start))
         ((< size -1)
          (signal 'redis-protocol-error
                  (list (format "Invalid Redis bulk string length: %d" size))))
         ((> message-end (length bytes)) redis--incomplete)
         ((not (and (= (aref bytes body-end) ?\r)
                    (= (aref bytes (1+ body-end)) ?\n)))
          (signal 'redis-protocol-error
                  (list "Redis bulk string is not terminated by CRLF")))
         (t
          (cons (substring bytes body-start body-end) message-end)))))))

(defun redis--parse-array (bytes start)
  "Return (VALUE . NEXT) for a RESP array in BYTES from START."
  (let ((length-line (redis--parse-number-line bytes start)))
    (if (eq length-line redis--incomplete)
        redis--incomplete
      (pcase-let ((`(,size . ,pos) length-line))
        (cond
         ((= size -1) (cons nil pos))
         ((< size -1)
          (signal 'redis-protocol-error
                  (list (format "Invalid Redis array length: %d" size))))
         (t
          (cl-loop repeat size
                   for parsed = (redis--parse-response bytes pos)
                   when (eq parsed redis--incomplete)
                   return redis--incomplete
                   collect (car parsed) into values
                   do (setq pos (cdr parsed))
                   finally return (cons values pos))))))))

(defun redis--parse-response (bytes &optional start)
  "Return (VALUE . NEXT) for one RESP response in BYTES.
START is a zero-based byte offset."
  (let ((start (or start 0)))
    (if (>= start (length bytes))
        redis--incomplete
      (pcase (aref bytes start)
        (?+ (redis--parse-simple-string bytes start))
        (?- (redis--parse-error bytes start))
        (?: (redis--parse-number-line bytes start))
        (?$ (redis--parse-bulk-string bytes start))
        (?* (redis--parse-array bytes start))
        (prefix
         (signal 'redis-protocol-error
                 (list (format "Unknown Redis response prefix: %c" prefix))))))))

(defun redis-parse-response (bytes)
  "Parse one complete RESP response from BYTES.
Return a cons cell (VALUE . CONSUMED-BYTES).  Signal
`redis-protocol-error' when BYTES do not contain one complete response."
  (let ((parsed (redis--parse-response
                 (if (multibyte-string-p bytes)
                     (encode-coding-string bytes 'binary t)
                   bytes))))
    (if (eq parsed redis--incomplete)
        (signal 'redis-protocol-error (list "Incomplete Redis response"))
      (when (redis--error-reply-p (car parsed))
        (signal 'redis-error (list (cdr (car parsed)))))
      parsed)))

;;;; Command execution

(defun redis--response-bytes (process start)
  "Return process buffer bytes from START for PROCESS."
  (with-current-buffer (process-buffer process)
    (buffer-substring-no-properties start (point-max))))

(defun redis--discard-response-bytes (process end)
  "Delete consumed response bytes through END from PROCESS buffer."
  (with-current-buffer (process-buffer process)
    (delete-region (point-min) end)
    (process-put process 'redis-response-start (point-min))))

(defun redis--read-response (conn)
  "Read one Redis response for CONN."
  (let* ((process (redis-conn-process conn))
         (deadline (+ (float-time) redis-response-timeout))
         parsed)
    (redis--ensure-live conn)
    (while (not parsed)
      (let ((remaining (- deadline (float-time))))
        (when (<= remaining 0)
          (signal 'redis-timeout-error
                  (list (format "Redis response timed out after %.3f seconds"
                                redis-response-timeout))))
        (accept-process-output process (min remaining 0.05) nil t))
      (redis--ensure-live conn)
      (let* ((start (or (process-get process 'redis-response-start)
                        (with-current-buffer (process-buffer process)
                          (point-min))))
             (bytes (redis--response-bytes process start))
             (next (redis--parse-response bytes 0)))
        (unless (eq next redis--incomplete)
          (setq parsed next)
          (redis--discard-response-bytes process (+ start (cdr next))))))
    (let ((value (car parsed)))
      (if (redis--error-reply-p value)
          (signal 'redis-error (list (cdr value)))
        value))))

(defun redis-command (conn command &rest arguments)
  "Send COMMAND with ARGUMENTS on CONN and return the Redis response.
Redis error responses signal `redis-error'.  Bulk strings are returned as
unibyte byte strings."
  (redis--ensure-live conn)
  (when (redis-conn-busy conn)
    (signal 'redis-connection-error
            (list "Redis connection is already running a command")))
  (setf (redis-conn-busy conn) t)
  (unwind-protect
      (progn
        (process-send-string
         (redis-conn-process conn)
         (apply #'redis-encode-command command arguments))
        (redis--read-response conn))
    (setf (redis-conn-busy conn) nil)))

(defun redis--maybe-authenticate (conn params)
  "Authenticate CONN when PARAMS include a password."
  (when-let* ((password (plist-get params :password)))
    (if-let* ((username (plist-get params :user)))
        (redis-command conn "AUTH" username password)
      (redis-command conn "AUTH" password))))

(defun redis--maybe-select-database (conn params)
  "Select the Redis logical database from PARAMS on CONN when present."
  (when-let* ((database (plist-get params :database)))
    (redis-command conn "SELECT" database)
    (setf (redis-conn-database conn) database)))

(defun redis-connect (params)
  "Connect to Redis using PARAMS and return a `redis-conn'.
PARAMS is a plist supporting :host, :port, :user, :password, and :database."
  (let* ((host (or (plist-get params :host) redis-default-host))
         (port (or (plist-get params :port) redis-default-port))
         (buffer (redis--make-buffer host port))
         conn)
    (condition-case err
        (let ((process (make-network-process
                        :name (redis--buffer-name host port)
                        :buffer buffer
                        :host host
                        :service port
                        :nowait nil
                        :noquery t
                        :coding 'binary)))
          (set-process-query-on-exit-flag process nil)
          (set-process-coding-system process 'binary 'binary)
          (process-put process 'redis-response-start
                       (with-current-buffer buffer (point-min)))
          (setq conn (make-redis-conn
                      :process process
                      :host host
                      :port port
                      :database (plist-get params :database)
                      :username (plist-get params :user)))
          (redis--maybe-authenticate conn params)
          (redis--maybe-select-database conn params)
          conn)
      (redis-error
       (when conn (redis-disconnect conn))
       (unless conn (kill-buffer buffer))
       (signal (car err) (cdr err)))
      (error
       (when conn (redis-disconnect conn))
       (unless conn (kill-buffer buffer))
       (signal 'redis-connection-error
               (list (error-message-string err)))))))

(defun redis-disconnect (conn)
  "Close Redis connection CONN."
  (when (redis-conn-p conn)
    (let ((process (redis-conn-process conn)))
      (setf (redis-conn-closed conn) t)
      (when (processp process)
        (let ((buffer (process-buffer process)))
          (delete-process process)
          (when (buffer-live-p buffer)
            (kill-buffer buffer))))))
  nil)

(provide 'redis)
;;; redis.el ends here
