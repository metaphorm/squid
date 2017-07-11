;;; Copyright 2017 Metaphorm Solutions, Inc.
;;;
;;; Licensed under the Apache License, Version 2.0 (the "License");
;;; you may not use this file except in compliance with the License.
;;; You may obtain a copy of the License at
;;;
;;; http://www.apache.org/licenses/LICENSE-2.0
;;;
;;; Unless required by applicable law or agreed to in writing, software
;;; distributed under the License is distributed on an "AS IS" BASIS,
;;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;; See the License for the specific language governing permissions and
;;; limitations under the License.

(library (stash)
  (export date-string->date
          hash)
  (import (scheme))

  (define-syntax hash
    (syntax-rules ()
      ((_ (key value) ...)
       (let ((hash-table (make-hash-table)))
         (hashtable-set! hash-table 'key value) ...
         hash-table))))

  (define (date-string->date date-string)
    (let ((part (lambda (start end)
                  (string->number
                   (substring date-string start end)))))
      (let ((year (part 0 4))
            (month (part 5 7))
            (day (part 8 10))
            (hour (part 11 13))
            (minute (part 14 16))
            (second (part 17 19)))
        (make-date 0 second minute hour day month year 0)))))
