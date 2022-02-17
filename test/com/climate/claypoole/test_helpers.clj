(ns com.climate.claypoole.test-helpers)

(defn eval+ex-unwrap
  "Test helper.
   Clojure 1.10 throws CompilerException from eval.
   We unwrap that exception to get the original."
  [code]
  (try
    (eval code)
    (catch clojure.lang.Compiler$CompilerException e
      (let [cause (.getCause e)]
        ;; Update the stack trace to include e
        (.setStackTrace cause (into-array StackTraceElement
                                          (concat
                                           (.getStackTrace cause)
                                           (.getStackTrace e))))
        (throw cause)))))
