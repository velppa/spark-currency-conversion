(ns rates
  (:require [cheshire.core :as json]
            [clojure.string :as str]))


(->> "rates.json"
     slurp
     (#(json/decode % true))
     :quotes
     (map (fn [[pair quote]]
            (format "%s,%s" (subs (name pair) 3) (double (/ 1 quote)))))
     (cons "currency,rate")
     (str/join "\n")
     (spit "rates.csv")
     )


;; (* 30.5 24 60 60)
