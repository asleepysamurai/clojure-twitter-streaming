(ns twitter.streaming
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [oauth.client :as oauth]
            [oauth.signature]
            [http.async.client :as c]))

(declare stream-handler)
(declare start-stream)

(def *oauth-consumer* nil)
(def *oauth-access-token* nil)
(def *oauth-access-token-secret* nil)
(def *protocol* "http")

(defmacro with-oauth
  "Set the OAuth access token to be used for all contained Twitter requests."
  [consumer access-token access-token-secret & body]
  `(binding [*oauth-consumer* ~consumer
             *oauth-access-token* ~access-token
             *oauth-access-token-secret* ~access-token-secret]
     (do 
       ~@body)))

(defmacro with-https
  [ & body]
  `(binding [*protocol* "https"]
     (do 
       ~@body)))

(defmacro def-twitter-streaming-method
  "Given basic specifications of a Twitter API method, build a function that will
   take any required and optional arguments and call the associated Twitter method."
  [method-name req-method req-url required-params optional-params]
  (let [required-fn-params (vec (sort (map #(symbol (name %))
                                           required-params)))
        optional-fn-params (vec (sort (map #(symbol (name %))
                                           optional-params)))]
    `(defn ~method-name
       [callback# ~@required-fn-params & rest#]
       (let [req-uri# (str *protocol* "://" ~req-url)
             rest-map# (apply hash-map rest#)
             provided-optional-params# (set/intersection (set ~optional-params)
                                                         (set (keys rest-map#)))
             required-query-param-names# (map (fn [x#]
                                                (keyword (string/replace (name x#) #"-" "_" )))
                                              ~required-params)
             optional-query-param-names-mapping# (map (fn [x#]
                                                        [x# (keyword (string/replace (name x#) #"-" "_"))])
                                                      provided-optional-params#)
             query-params# (merge (apply hash-map
                                         (vec (interleave required-query-param-names# ~required-fn-params)))
                                  (apply merge
                                         (map (fn [x#] {(second x#) ((first x#) rest-map#)}) optional-query-param-names-mapping#)))
             need-to-url-encode# (into {} (map (fn [[k# v#]] [k# (oauth.signature/url-encode v#)]) query-params#))
             oauth-creds# (when (and *oauth-consumer* 
                                     *oauth-access-token*) 
                            (oauth/credentials *oauth-consumer*
                                               *oauth-access-token*
                                               *oauth-access-token-secret*
                                               ~req-method
                                               req-uri#
                                               query-params#))]
         (start-stream req-uri# (into (sorted-map) (merge query-params# oauth-creds#)) ~req-method callback#)))))

(def-twitter-streaming-method statuses-filter
	:post
	"stream.twitter.com/1/statuses/filter.json"
	[]
	[:count :delimited :follow :locations :track])

(def-twitter-streaming-method statuses-firehose
	:get
	"stream.twitter.com/1/statuses/firehose.json"
	[]
	[:count :delimited])

(def-twitter-streaming-method statuses-links
	:get
	"stream.twitter.com/1/statuses/links.json"
	[]
	[:count :delimited])

(def-twitter-streaming-method statuses-retweet
	:get
	"stream.twitter.com/1/statuses/retweets.json"
	[]
	[:delimited])

(def-twitter-streaming-method statuses-sample
	:get
	"stream.twitter.com/1/statuses/sample.json"
	[]
	[:count :delimited])

(defn start-stream [uri query req-method callback]
	(println query)
	(let [resp (if (= :post req-method)
				   (c/stream-seq :post uri :body query)
				   (c/stream-seq :get uri :query query))]
		 (println (c/status resp))							 
		 (stream-handler resp callback)
		 ))

(defn stream-handler
  "Handle the various HTTP status codes that may be returned when accessing
the Twitter API."
  [resp callback]
  (let [status (c/status resp)
  	    result (c/string resp)]	
	   (if-not (> (:code status) 200) 
	  		   (doseq [twit-str result] (callback twit-str))
	  		   (throw (proxy [Exception] [(str "Twitter threw a " (:code status) ":" (:msg status) " exception.")]))
	  		   )))
	  		   