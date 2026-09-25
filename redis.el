;;; redis.el --- Native Redis RESP client -*- lexical-binding: t; -*-

;; Copyright (C) 2026 Lucius Chen
;; SPDX-License-Identifier: GPL-3.0-or-later

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Assisted-by: OpenAI Codex:gpt-5.6-sol, Claude code:fable-5
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.1
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

(defcustom redis-connect-timeout 3
  "Seconds to wait for a Redis TCP connection to open."
  :type 'number
  :group 'redis)

(defcustom redis-max-response-bytes (* 64 1024 1024)
  "Maximum bytes accepted in one RESP response."
  :type 'integer
  :group 'redis)

(defcustom redis-max-bulk-bytes (* 32 1024 1024)
  "Maximum bytes accepted in one RESP bulk string."
  :type 'integer
  :group 'redis)

(defcustom redis-max-elements 1000000
  "Maximum total declared array elements in one RESP response."
  :type 'integer
  :group 'redis)

(defcustom redis-max-depth 128
  "Maximum nested RESP array depth."
  :type 'integer
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

;;;; RESP decoding

(defconst redis--int64-min (- (expt 2 63))
  "Minimum RESP signed integer value.")

(defconst redis--int64-max (1- (expt 2 63))
  "Maximum RESP signed integer value.")

(defun redis--decode-line (bytes)
  "Decode RESP line BYTES as UTF-8 text."
  (decode-coding-string bytes 'utf-8 t))

(defun redis-decode-string (bytes &optional coding)
  "Decode Redis bulk BYTES using CODING, defaulting to UTF-8.
If BYTES is nil, return nil."
  (when bytes
    (decode-coding-string bytes (or coding 'utf-8) t)))

(defun redis--parse-integer-token (payload)
  "Return signed 64-bit integer encoded by RESP PAYLOAD."
  (unless (and (<= (length payload) 20)
               (string-match-p "\\`-?[0-9]+\\'" payload))
    (signal 'redis-protocol-error (list "Invalid Redis integer token")))
  (let ((value (string-to-number payload)))
    (unless (<= redis--int64-min value redis--int64-max)
      (signal 'redis-protocol-error (list "Redis integer is outside signed 64-bit range")))
    value))

(defun redis--decode-response (bytes)
  "Return the value of the one RESP response in BYTES.
`redis--scan-available' validated BYTES while reading them, so this only
builds the value.  Signal `redis-error' for the first error reply."
  (let ((pos 0))
    (cl-labels
        ((line ()
           (let ((end (string-search "\r\n" bytes pos)))
             (prog1 (substring bytes (1+ pos) end)
               (setq pos (+ end 2)))))
         (value ()
           (pcase (aref bytes pos)
             (?+ (redis--decode-line (line)))
             (?- (signal 'redis-error (list (redis--decode-line (line)))))
             (?: (redis--parse-integer-token (line)))
             (?$ (let ((size (redis--parse-integer-token (line))))
                   (unless (= size -1)
                     (prog1 (substring bytes pos (+ pos size))
                       (setq pos (+ pos size 2))))))
             (?* (let ((size (redis--parse-integer-token (line))))
                   (unless (= size -1)
                     (cl-loop repeat size collect (value))))))))
      (value))))

;;;; Command execution

(cl-defstruct (redis--scan-state
               (:constructor redis--make-scan-state))
  "Incremental RESP envelope scan state."
  pos
  stack
  line-search
  (elements 0)
  complete)

(defun redis--scan-line (start &optional state)
  "Return (PAYLOAD . NEXT) for a complete buffer line at START.
When STATE is non-nil, resume searching after the previously scanned bytes."
  (save-excursion
    (goto-char (or (and state (redis--scan-state-line-search state))
                   (1+ start)))
    (if (search-forward "\r\n" nil t)
        (progn
          (when state (setf (redis--scan-state-line-search state) nil))
          (cons (buffer-substring-no-properties (1+ start) (- (point) 2))
                (point)))
      (when state
        ;; Recheck one byte so a CR/LF split across chunks is recognized.
        (setf (redis--scan-state-line-search state)
              (max (1+ start) (1- (point-max)))))
      nil)))

(defun redis--scan-number-line (start &optional state)
  "Return (NUMBER . NEXT) for a complete numeric line at START.
STATE, when non-nil, preserves incremental line scan progress."
  (when-let* ((line (redis--scan-line start state)))
    (cons (redis--parse-integer-token (car line)) (cdr line))))

(defun redis--process-sentinel (process event)
  "Record Redis PROCESS termination EVENT without changing its wire buffer."
  (unless (redis--process-live-p process)
    (process-put process 'redis-error (string-trim event))))

(defun redis--scan-complete-value (state next)
  "Record one complete value ending at NEXT in scan STATE."
  (setf (redis--scan-state-pos state) next)
  (let ((propagate t))
    (while propagate
      (if-let* ((stack (redis--scan-state-stack state)))
          (let ((remaining (1- (car stack))))
            (if (> remaining 0)
                (progn
                  (setcar stack remaining)
                  (setq propagate nil))
              (setf (redis--scan-state-stack state) (cdr stack))))
        (setf (redis--scan-state-complete state) next)
        (setq propagate nil)))))

(defun redis--scan-available (state)
  "Incrementally scan available bytes in current buffer using STATE.
Return the absolute end position when one complete response is available."
  (cl-block scan
    (while (and (not (redis--scan-state-complete state))
                (< (redis--scan-state-pos state) (point-max)))
      (let* ((start (redis--scan-state-pos state))
             (prefix (char-after start)))
        (pcase prefix
        ((or ?+ ?- ?:)
         (if-let* ((line (if (= prefix ?:)
                             (redis--scan-number-line start state)
                           (redis--scan-line start state))))
             (redis--scan-complete-value state (cdr line))
           (cl-return-from scan nil)))
        (?$
         (if-let* ((line (redis--scan-number-line start state)))
             (pcase-let* ((`(,size . ,body-start) line)
                          (body-end (+ body-start size))
                          (message-end (+ body-end 2)))
               (cond
                ((= size -1)
                 (redis--scan-complete-value state body-start))
                ((< size -1)
                 (signal 'redis-protocol-error
                         (list (format "Invalid Redis bulk string length: %d"
                                       size))))
                ((> size redis-max-bulk-bytes)
                 (signal 'redis-protocol-error
                         (list (format "Redis bulk string exceeds %d-byte limit"
                                       redis-max-bulk-bytes))))
                ((> message-end (point-max)) (cl-return-from scan nil))
                ((not (and (= (char-after body-end) ?\r)
                           (= (char-after (1+ body-end)) ?\n)))
                 (signal 'redis-protocol-error
                         (list "Redis bulk string is not terminated by CRLF")))
                (t (redis--scan-complete-value state message-end))))
           (cl-return-from scan nil)))
        (?*
         (if-let* ((line (redis--scan-number-line start state)))
             (pcase-let ((`(,size . ,next) line))
               (cond
                ((= size -1) (redis--scan-complete-value state next))
                ((< size -1)
                 (signal 'redis-protocol-error
                         (list (format "Invalid Redis array length: %d" size))))
                ((>= (length (redis--scan-state-stack state)) redis-max-depth)
                 (signal 'redis-protocol-error
                         (list (format "Redis response exceeds depth limit %d"
                                       redis-max-depth))))
                ((= size 0) (redis--scan-complete-value state next))
                (t
                 (cl-incf (redis--scan-state-elements state) size)
                 (when (> (redis--scan-state-elements state) redis-max-elements)
                   (signal 'redis-protocol-error
                           (list (format "Redis response exceeds %d-element limit"
                                         redis-max-elements))))
                 (setf (redis--scan-state-pos state) next)
                 (push size (redis--scan-state-stack state)))))
           (cl-return-from scan nil)))
        (_
         (signal 'redis-protocol-error
                 (list (format "Unknown Redis response prefix: %c" prefix))))))))
  (redis--scan-state-complete state))

(defun redis--wait-for-connect (process)
  "Wait until PROCESS opens, bounded by `redis-connect-timeout'."
  (let ((deadline (+ (float-time) redis-connect-timeout)))
    (while (eq (process-status process) 'connect)
      (let ((remaining (- deadline (float-time))))
        (when (<= remaining 0)
          (signal 'redis-connection-error
                  (list (format "Redis connection timed out after %.3f seconds"
                                redis-connect-timeout))))
        ;; On macOS, restricting output to an asynchronous network process can
        ;; prevent its connect event from being dispatched at all.
        (accept-process-output nil (min remaining 0.05))))
    (unless (redis--process-live-p process)
      (signal 'redis-connection-error
              (list (format "Redis connection failed: %s"
                            (string-trim
                             (format "%s" (or (process-get process 'redis-error)
                                               (process-status process))))))))))

(defun redis--discard-response-bytes (process end)
  "Delete consumed response bytes through END from PROCESS buffer."
  (with-current-buffer (process-buffer process)
    (delete-region (point-min) end)
    (process-put process 'redis-response-start (point-min))))

(defun redis--read-response (conn)
  "Read one Redis response for CONN."
  (let* ((process (redis-conn-process conn))
         (buffer (process-buffer process))
         (start (or (process-get process 'redis-response-start)
                    (with-current-buffer buffer (point-min))))
         (state (redis--make-scan-state :pos start))
         (deadline (+ (float-time) redis-response-timeout))
         end)
    (while (not end)
      (setq end
            (with-current-buffer buffer
              (when (> (- (point-max) start) redis-max-response-bytes)
                (signal 'redis-protocol-error
                        (list (format "Redis response exceeds %d-byte limit"
                                      redis-max-response-bytes))))
              (redis--scan-available state)))
      (unless end
        (let ((remaining (- deadline (float-time))))
          (redis--ensure-live conn)
          (when (<= remaining 0)
            (signal 'redis-timeout-error
                    (list (format "Redis response timed out after %.3f seconds"
                                  redis-response-timeout))))
          (accept-process-output process (min remaining 0.05) nil t))))
    (let ((bytes (with-current-buffer buffer
                   (buffer-substring-no-properties start end))))
      (redis--discard-response-bytes process end)
      (redis--decode-response bytes))))

(defun redis-command (conn command &rest arguments)
  "Send COMMAND with ARGUMENTS on CONN and return the Redis response.
Redis error responses signal `redis-error'.  Bulk strings are returned as
unibyte byte strings."
  (redis--ensure-live conn)
  (when (redis-conn-busy conn)
    (signal 'redis-connection-error
            (list "Redis connection is already running a command")))
  (let ((payload (apply #'redis-encode-command command arguments))
        completed)
    (setf (redis-conn-busy conn) t)
    (unwind-protect
        (condition-case err
            ;; Bind throw-on-input to nil so `while-no-input', used by
            ;; completion frameworks to abandon slow candidate lookups, cannot
            ;; abort the exchange between send and reply.
            (prog1 (let ((throw-on-input nil))
                     (process-send-string (redis-conn-process conn) payload)
                     (redis--read-response conn))
              (setq completed t))
          ((redis-timeout-error redis-protocol-error redis-connection-error)
           (setq completed t)
           (redis-disconnect conn)
           (signal (car err) (cdr err)))
          (redis-error
           (setq completed t)
           (signal (car err) (cdr err)))
          (quit
           (setq completed t)
           (redis-disconnect conn)
           (signal (car err) (cdr err)))
          (error
           (setq completed t)
           (redis-disconnect conn)
           (signal 'redis-connection-error
                   (list (error-message-string err)))))
      ;; RESP carries no request identifier, so an exchange abandoned by a
      ;; nonlocal exit would hand its reply to the next caller.
      (unless completed
        (redis-disconnect conn))
      (setf (redis-conn-busy conn) nil))))

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
         process
         conn)
    (unwind-protect
        (condition-case err
            (progn
              (setq process
                    (make-network-process
                     :name (redis--buffer-name host port)
                     :buffer buffer
                     :host host
                     :service port
                     :nowait t
                     :noquery t
                     :coding 'binary))
              (set-process-query-on-exit-flag process nil)
              (set-process-coding-system process 'binary 'binary)
              (redis--wait-for-connect process)
              ;; Keep connection establishment separate from protocol handling:
              ;; install the wire-safe sentinel before any Redis command is sent.
              (set-process-sentinel process #'redis--process-sentinel)
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
              ;; Transfer transport ownership to the returned connection.
              (prog1 conn (setq process nil buffer nil)))
          (redis-error
           (signal (car err) (cdr err)))
          (error
           (signal 'redis-connection-error
                   (list (error-message-string err)))))
      (when (processp process) (delete-process process))
      (when (buffer-live-p buffer) (kill-buffer buffer)))))

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
