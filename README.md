# sqsd by golang

## Motivation

This tool emulates [sqsd of AWS Elastic Beanstalk worker environments](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features-managing-env-tiers.html).

The concept of sqsd is simplifying worker process management by lightwaight languages such as Perl and PHP,  
and separating consuming process from SQS and job process.  
These languages has no defact standard worker libary such as Ruby's `sidekiq`,  
so it's difficult to build worker system or to manage it reliablly.

`sqsd` works only two things:

- Fetching queue message from SQS
    - also removing it when job is succeeded
- Invoking message to job process by **HTTP POST request**

Many languages' HTTP server library are stable, so user builds `worker server` by HTTP server.

This (github.com/taiyoh/sqsd) builds its concept without AWS Elastic Beanstalk.
