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

(library (squid)
  (export connect
          connected?
          query
          disconnect)
  (import (mysql-api)
          (stash)
          (scheme))

  (define (connect host port database user password)
    (let ((connection (mysql-init (make-ftype-pointer mysql #x00000000))))
      (if (ftype-pointer-null? connection)
          (begin
            (display (mysql-error connection))
            connection)
          (if (ftype-pointer-null?
               (mysql-real-connect connection host user password database port #f 0))
              (begin
                (display (mysql-error connection))
                (mysql-close connection)
                connection)
              connection))))

  (define (query connection statement)
    (if (not (zero? (mysql-query connection statement)))
        (display (mysql-error connection))
        (let ((result (mysql-use-result connection)))
          (if (not (ftype-pointer-null? result))
              (let* ((num-fields (mysql-num-fields result))
                     (fields (parse-fields result num-fields))
                     (rows (parse-rows result fields)))
                (mysql-free-result result)
                (list fields rows))))))

  (define (parse-fields result num-fields)
    (mysql-field-seek result 0)
    (let ((fields (make-vector num-fields)))
      (let parse-field ((field-index 0))
        (let ((field (mysql-fetch-field result)))
          (if (ftype-pointer-null? field)
              fields
              (begin
                (vector-set! fields field-index (cons (field-name field)
                                                      (field-type field)))
                (parse-field (+ field-index 1))))))))

  (define (parse-rows result fields)
    (let ((rows (list)))
      (let parse-row ()
        (let ((row (mysql-fetch-row result)))
          (if (ftype-pointer-null? row)
              (reverse rows)
              (begin
                (set! rows (cons (parse-columns row fields) rows))
                (parse-row)))))))

  (define (parse-columns row fields)
    (let* ((num-fields (vector-length fields))
           (row-vector (make-vector num-fields)))
      (let parse-column ((column-index 0))
        (if (= column-index num-fields)
            row-vector
            (let ((type (cdr (vector-ref fields column-index))))
              (vector-set! row-vector
                           column-index
                           (parse-cell row column-index type))
              (parse-column (+ column-index 1)))))))

  (define (parse-cell row column-index type)
    (if (null-cell? row column-index)
        'null
        (type-convert (cell-string row column-index) type)))

  (define (type-convert value type)
    (case type
      ((decimal tiny short long float double longlong int24 newdecimal)
       (string->number value))
      ((datetime)
       (date-string->date value))
      (else value)))

  (define (connected? connection)
    (eq? (mysql-ping connection) 0))

  (define (disconnect connection)
    (mysql-close connection)))
