POC processing JSON logs on kafka using Spark Streaming

### Features
- Parse marker structures | {ENTER, [{FLOW}]} becomes FLOW,ENTER
  + Duplicate rows for each ancestor path 
  + Reverse the order child > parents into parent.child to go for the most generic marker first   
  
- Parse properties like messages | ex: log.info("project.function.var=xxx") 