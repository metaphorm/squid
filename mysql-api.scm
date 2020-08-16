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

(library (mysql-api)
  (export mysql
          mysql-close
          mysql-error
          mysql-fetch-field
          mysql-fetch-row
          mysql-field
          mysql-field-seek
          mysql-free-result
          mysql-init
          mysql-num-fields
          mysql-ping
          mysql-query
          mysql-real-connect
          mysql-res
          mysql-row
          mysql-use-result
          cell-string
          field-name
          field-type
          null-cell?)
  (import (scheme)
          (stash))

  ;; https://dev.mysql.com/doc/refman/5.7/en/c-api-function-overview.html
  (define libmysql (load-shared-object "libmysqlclient.so"))

  (define-ftype mysql
    (struct))

  (define-ftype mysql-res
    (struct))

  (define-ftype mysql-field
    (struct
      (name (* char))
      (org_name (* char))
      (table (* char))
      (org_table (* char))
      (db (* char))
      (catalog (* char))
      (def (* char))
      (length unsigned-long)
      (max_length unsigned-long)
      (name_length unsigned-int)
      (org_name_length unsigned-int)
      (table_length unsigned-int)
      (org_table_length unsigned-int)
      (db_length unsigned-int)
      (catalog_length unsigned-int)
      (def_length unsigned-int)
      (flags unsigned-int)
      (decimals unsigned-int)
      (charsetnr unsigned-int)
      (type unsigned-8)
      (extension void*)))

  (define-ftype mysql-row (* char))

  (define mysql-init
    (foreign-procedure "mysql_init" ((* mysql)) (* mysql)))

  (define mysql-real-connect
    (foreign-procedure
     "mysql_real_connect"
     ((* mysql) string string string string unsigned-int string unsigned-long)
     (* mysql)))

  (define mysql-close
    (foreign-procedure "mysql_close" ((* mysql)) void))

  (define mysql-use-result
    (foreign-procedure "mysql_use_result" ((* mysql)) (* mysql-res)))

  (define mysql-free-result
    (foreign-procedure "mysql_free_result" ((* mysql-res)) void))

  (define mysql-fetch-row
    (foreign-procedure "mysql_fetch_row" ((* mysql-res)) (* mysql-row)))

  (define mysql-fetch-field
    (foreign-procedure "mysql_fetch_field" ((* mysql-res)) (* mysql-field)))

  (define mysql-field-seek
    (foreign-procedure "mysql_fetch_fields" ((* mysql-res) unsigned-int) unsigned-int))

  (define mysql-num-fields
    (foreign-procedure "mysql_num_fields" ((* mysql-res)) unsigned-int))

  (define mysql-query
    (foreign-procedure "mysql_query" ((* mysql) string) int))

  (define mysql-error
    (foreign-procedure "mysql_error" ((* mysql)) string))

  (define mysql-ping
    (foreign-procedure "mysql_ping" ((* mysql)) int))

  (define field-type-map
    (hash (0 'decimal)
          (1 'tiny)
          (2 'short)
          (3 'long)
          (4 'float)
          (5 'double)
          (6 'null)
          (7 'timestamp)
          (8 'longlong)
          (9 'int24)
          (10 'date)
          (11 'time)
          (12 'datetime)
          (13 'year)
          (14 'newdate)
          (15 'varchar)
          (16 'bit)
          (17 'timestamp2)
          (18 'datetime2)
          (19 'time2)
          (245 'json)
          (246 'newdecimal)
          (247 'enum)
          (248 'set)
          (249 'tiny-blob)
          (250 'medium-blob)
          (251 'long-blob)
          (252 'blob)
          (253 'var-string)
          (254 'string)
          (255 'geometry)))

  (define (field-type-symbol field-type-index)
    (hashtable-ref field-type-map field-type-index '()))

  (define (field-type field)
    (field-type-symbol
     (foreign-ref 'unsigned-8
                  (ftype-pointer-address
                   (ftype-&ref mysql-field (type) field 0)) 0)))

  (define (field-name field)
    (char*->string
     (ftype-pointer-address
      (ftype-&ref mysql-field (name 0) field 0))))

  (define (null-cell? row column-index)
    (eq? 0 (foreign-ref 'unsigned-8
                        (ftype-pointer-address
                         (ftype-&ref mysql-row () row column-index)) 0)))

  (define (cell-string row column-index)
    (char*->string
     (ftype-pointer-address
      (ftype-&ref mysql-row (0) row column-index))))

  (define (char*->string address)
    (list->string
     (reverse
      (let loop ((pointer address) (chars '()))
        (let ((char (foreign-ref 'unsigned-8 pointer 0)))
          (if (equal? char 0)
              chars
              (loop (+ 1 pointer) (cons (integer->char char) chars)))))))))
