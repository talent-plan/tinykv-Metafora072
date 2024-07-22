## 介绍

**4A：** 本部分是对 mvcc 模块的实现，供 project4B/C 调用，主要涉及修改的代码文件是 transaction.go。需要利用对 CFLock, CFDefault 和 CFWrite 三个 CF 的一些操作来实现 mvcc。

**4B：** project4B 主要实现事务的两段提交，即 prewrite 和 commit，需要完善的文件是 server.go。要注意的是，这要需要通过 server.Latches 对 keys 进行加锁。

**4C：**



## 记录

4A `transaction.go`中的 CurrentWrite函数，为什么要判断？

![](project4.assets/QQ截图20240722220046.png)

4B 注意 Region error

![](project4.assets/QQ截图20240723002110.png)



4B KvGet方法，图中如果不判断 RegionError 会怎样？

![](project4.assets/QQ截图20240723003649.png)

4B KvCommit方法，图中如果不判断会怎样？

![](project4.assets/QQ截图20240723022253.png)

4B KvCommit方法，图中如果不判断会怎样？

![](project4.assets/QQ截图20240723022822.png)

## 说明

![	](project4.assets/QQ截图20240722230151.png)