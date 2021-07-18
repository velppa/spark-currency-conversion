(ns rates
  (:require [cheshire.core :as json]
            [org.httpkit.client :as http]
            [clojure.string :as str]))


(defn interval
  "Return validity interval starging after provided timestamp."
  [timestamp]
  (let [length (* 4 7 24 60 60)         ; 4 weeks in seconds
        div (int (/ timestamp length))
        effective-from (* (inc div) length)
        effective-to (+ effective-from length)]
    [effective-from effective-to]))


(defn increment
  "Download live exchange rates from currencylayer.com
and turns them into increment."
  [timestamp]
  (let [api-key (System/getenv "CURRENCYLAYER_API_KEY")
        response @(http/request
                   {:method       :get
                    :url          "http://api.currencylayer.com/live"
                    :query-params {:access_key api-key}})
        rates (json/decode (:body response) true)
        ;; rates (json/decode (slurp "rates.json") true)
        [effective-from effective-to] (interval timestamp)]
    (->> rates
         :quotes
         (map (fn [[pair rate]]
                [(subs (name pair) 3)
                 (double (/ 1 rate))
                 timestamp
                 effective-from
                 effective-to])))))



(defn old-data
  "Return sequence of old data from filename without those entries
that start with increment's effective-from."
  [filename timestamp]
  (->> filename
       slurp
       str/split-lines
       (map (fn [line]
              (let [[curr rate ts from to] (str/split line #",")]
                [curr
                 (Double/parseDouble rate)
                 (Integer/parseInt ts)
                 (Integer/parseInt from)
                 (Integer/parseInt to)])))
       (filter (fn [[_ _ _ effective-from _]]
                 (not= effective-from timestamp)))))


(defn -main [{:keys [timestamp filename]
              :or   {timestamp (int (/ (System/currentTimeMillis) 1000))}}]
  (->>
   (concat (increment timestamp) (old-data filename timestamp))
   (map #(str/join "," %))
   (str/join "\n")
   (spit filename)))


(comment

  ;; generate history of currency rates
  (let [now (int (/ (System/currentTimeMillis) 1000))
        timestamps (map (fn [i]
                          {:filename "rates.csv"
                           :timestamp (- now (* i 4 7 24 60 60))}) (take 40 (range)))]
    (doall (map -main timestamps)))

  (->> "rates.json"
       slurp
       (#(json/decode % true))
       :quotes
       (map (fn [[pair quote]]
              (format "%s,%s" (subs (name pair) 3) (double (/ 1 quote)))))
       (cons "currency,rate")
       (str/join "\n")
       (spit "rates.csv"))


  (* 30.5 24 60 60)

  (int (/ 1626436324 (* 4 7 24 60 60))))
