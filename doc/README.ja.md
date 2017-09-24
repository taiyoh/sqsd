# sqsdとは

AWS Elastic Beanstalkには「ワーカー環境」というものがあり、  
SQSを使ったjob queueシステムをシンプルに組むことができます。  
http://docs.aws.amazon.com/ja_jp/elasticbeanstalk/latest/dg/using-features-managing-env-tiers.html  

gearman workerのようにjob queueサーバにworkerを接続するのではなく、SQSからのMessageの受け取りと、受け取ったMessageを実際に処理を行うHTTPサーバに送る部分だけをsqsdは担当しています。  
これにより、job queueシステムとビジネスロジックが結合することなくシステムを構築することができます。

特に、RubyやPerl、Java等の他の言語によって既にアプリケーションが構築されている場合においても、golangとの言語間のデータのやりとりを気にすることがなくなるのが大きな利点です。
