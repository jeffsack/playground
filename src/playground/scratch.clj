
; is log4j configured?
(.hasMoreElements (.getAllAppenders (org.apache.log4j.Logger/getRootLogger)))

; mucking with log4j levels
(.setLevel (org.apache.log4j.Logger/getRootLogger) org.apache.log4j.Level/DEBUG)


; get a detailed trace of the last error in the repl
(use 'clojure.stacktrace)
(print-stack-trace *e)

