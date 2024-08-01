## 记录

文档解释得很详细





## 问题

`removeNode` 方法，删除结点如果不处理可能的日志提交会怎样？

![](project3A.assets/QQ截图20240725190114.png)

`handleTransferLeader` 方法，如果不判断转移目标是自身会怎样？

![](project3A.assets/QQ截图20240725190207.png)

要注意 `handlePropose` 方法时，leader 处于领导权变更，要停止接收新的请求

![](project3A.assets/QQ截图20240725195313.png)

![](project3A.assets/QQ截图20240725195408.png)