{:auth
 {:repository-auth
  {#"clojars"
   {:username #=(eval (System/getenv "CLOJARS_USER"))
    :password #=(eval (System/getenv "CLOJARS_PASS"))}}}}
