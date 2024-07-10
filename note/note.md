# 1 Project1 StandaloneKV

## 1.1 项目指引文档

在这个项目中，你将构建一个支持列族的独立键值存储 gRPC 服务。独立意味着只有一个节点，而不是分布式系统。列族（简称 CF）是一个类似于键命名空间的术语，即不同列族中的同一个键的值是不同的。你可以简单地将多个列族视为单独的小型数据库。在项目4中你会明白为什么 TinyKV 需要 CF 的支持，因为它用于支持事务模型。

该服务支持四个基本操作：Put、Delete、Get、Scan。它维护一个简单的键值对数据库。键和值都是字符串。Put 替换指定列族中某个键的值，Delete 删除指定列族中某个键的值，Get 获取指定列族中某个键的当前值，Scan 获取指定列族中一系列键的当前值。

该项目可以分为两个步骤：

1. 实现独立存储引擎。
2. 实现原始键值服务处理程序。

### 代码结构

gRPC 服务器在 `kv/main.go` 中初始化，包含一个 tinykv.Server，它提供名为 TinyKv 的 gRPC 服务。该服务由 `proto/proto/tinykvpb.proto` 中的协议缓冲区定义，rpc 请求和响应的详细信息在 `proto/proto/kvrpcpb.proto` 中定义。

通常，你不需要更改 proto 文件，因为所有必要的字段都已为你定义。但是，如果你仍然需要更改，可以修改 proto 文件并运行 `make proto` 以更新 `proto/pkg/xxx/xxx.pb.go` 中相关的生成的 Go 代码。

此外，Server 依赖于一个 Storage 接口，你需要在 `kv/storage/standalone_storage/standalone_storage.go` 中实现该接口。一旦在 StandaloneStorage 中实现了 Storage 接口，你就可以使用它为 Server 实现原始键值服务。

### 实现独立存储引擎

第一个任务是实现 badger 键值 API 的包装器。gRPC 服务器的服务依赖于 `kv/storage/storage.go` 中定义的 Storage 接口。在此上下文中，独立存储引擎只是 badger 键值 API 的包装器，提供两种方法：

```go
type Storage interface {
    // 其他方法
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

Write 方法应提供一种将一系列修改应用于内部状态的方法，在这种情况下，内部状态是一个 badger 实例。

Reader 方法应返回一个支持快照中键值的点获取和扫描操作的 StorageReader。

你现在不需要考虑 kvrpcpb.Context，它会在后续项目中使用。

#### 提示：

1. 你应该使用 `badger.Txn` 来实现 Reader 函数，因为 badger 提供的事务处理程序可以提供键和值的一致快照。
2. Badger 不支持列族。`engine_util` 包（`kv/util/engine_util`）通过在键前添加前缀来模拟列族。例如，属于特定列族 cf 的键 key 存储为 `${cf}_${key}`。它包装了 badger 提供对 CF 的操作，并且还提供了许多有用的辅助函数。因此，你应该通过 `engine_util` 提供的方法进行所有读写操作。请阅读 `util/engine_util/doc.go` 了解更多信息。
3. TinyKV 使用原始版本 badger 的一个分支，并进行了一些修复，因此请使用 `github.com/Connor1996/badger` 而不是 `github.com/dgraph-io/badger`。
4. 不要忘记调用 `badger.Txn` 的 `Discard()` 并在丢弃之前关闭所有迭代器。

### 实现服务处理程序

项目的最后一步是使用实现的存储引擎构建原始键值服务处理程序，包括 `RawGet`、`RawScan`、`RawPut`、`RawDelete`。处理程序已经为你定义好了，你只需在 `kv/server/raw_api.go` 中填写实现即可。完成后，请记得运行 `make project1` 通过测试套件。

## 1.2 项目需求概述

本节项目要求可概括为以下两个部分：

1. 实现对底层 badger api 的包装，主要涉及修改的代码文件是 standalone_storage.go, 需要实现 Storage 接口的 **Write** 和 **Reader** 方法，来实现对底层 badger 数据库的读写。
2. 实现 **RawGet**，**RawScan**，**RawPut**，**RawDelete** 四个方法，主要涉及修改的代码文件是 raw_api.go

## 1.3 项目具体实现

### part1：实现独立存储引擎 StandAloneStorage

在 `standalone_storage.go`中，查看需要我们实现的部分，如下：

```go
type StandAloneStorage struct {
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
}
```

我们查看`storage.go`：

```go
// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

显而易见，我们需要实现的独立存储引擎`StandAloneStorage`就是要实现`Storage`接口。提示中说明，我们需要实现的独立存储引擎只是 badger 键值 API 的包装器。那么，我们的`StandAloneStorage`就是对`Badger`数据库的一层包装：

```go
type StandAloneStorage struct {
    db *badger.DB
}
```

`db`字段将用于与Badger数据库进行交互，执行各种存储操作。

参考Badger数据库的官方文档，我们可以实现`NewStandAloneStorage`方法，创建并初始化一个`StandAloneStorage`实例。

![](/imgsrc/1.png)

```go
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	db,err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return &StandAloneStorage{
		db : db,
	}
}
```

接下来，考察`Start`方法。由于我们在`NewStandAloneStorage`函数中完成了创建和初始化，所以这里不需要再做其它事情。

```go
func (s *StandAloneStorage) Start() error {
	return nil
}
```

考察`Stop`方法。参考官方文档可知，调用其`Close`方法就能关闭数据库，所以直接调用其`Close`方法就行了。

```go
func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}
```

接下来考虑实现其`Reader`方法。

```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
}
```

该方法要求返回一个`StorageReader`，`StorageReader`是一个在 `storage.go`中定义的接口：

```go
type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
```

根据提示，我们应该使用 badger.Txn 来实现 Reader 函数，所以我们需要声明了一个 badgerReader 结构体来实现 StorageReader 接口，badgerReader 结构体内部包含对 badger.Txn 的引用。

```go
// BadgerReader <StorageReader>
type BadgerReader struct {
	txn *badger.Txn
}
```

要实现 StorageReader 接口，就要实现其所有方法。根据提示，`engine_util` 包中已经实现了相关操作，我们直接调用就行了。

对于`GetCF`方法，该方法用于从指定的 CF 中获取给定键的值。我们直接调用`engine_util.GetCFFromTxn`即可：

```go
func (br *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	//func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error)
	value, err := engine_util.GetCFFromTxn(br.txn,cf,key)

	if err == badger.ErrKeyNotFound {
		return nil,nil
	}
	return value,err
}
```

注意，根据下图的测试结果，`ErrKeyNotFound`不是`err`，这里需要对`ErrKeyNotFound`进行特殊处理。

![](/imgsrc/2.png)

对于`IterCF`方法，该方法会创建一个新的列族迭代器，用于遍历指定列族中的所有键值对。可以调用`engine_util.NewCFIterator`实现：

```go
func (br *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	it := engine_util.NewCFIterator(cf,br.txn)
	return it
}
```

对于`Close`方法，其确保在使用完`BadgerReader`后，正确地关闭事务并释放资源。根据`Badger`的官方文档，使用其`Discard`函数丢弃事务即可。

```go
func (br *BadgerReader) Close() {
	br.txn.Discard()
}
```

至此，我们已经实现了 StorageReader 接口，接下来，通过创建一个新的只读事务，并返回一个`BadgerReader`实例，我们就实现了 Reader 方法：

```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &BadgerReader{txn}, nil
}
```

接下来考虑实现 `Write` 方法。该方法传入了一个包含多个`Modify`对象的切片`batch`。在`modify.go`中，可以找到对`Modify`的定义。

```go
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error
```

```go
// Modify is a single modification to TinyKV's underlying storage.
type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}
...
```

可以发现，Modify 对象代表着 Put 和 Delete 两种操作。这两种操作在`engine_util`已经实现好了。所以，我们只需要对每一个操作，从`engine_util`中调用相应的已经实现好的函数即可。

```go
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		key, value, cf := modify.Key(), modify.Value(), modify.Cf()
		var err error
		switch modify.Data.(type) {
		case storage.Put:
			// func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
			// 	return engine.Update(func(txn *badger.Txn) error {
			// 		return txn.Set(KeyWithCF(cf, key), val)
			// 	})
			// }
			err = engine_util.PutCF(s.db,cf,key,value)
		case storage.Delete:
			// func DeleteCF(engine *badger.DB, cf string, key []byte) error {
			// 	return engine.Update(func(txn *badger.Txn) error {
			// 		return txn.Delete(KeyWithCF(cf, key))
			// 	})
			// }
			err = engine_util.DeleteCF(s.db,cf,key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
```

通过遍历`batch`，处理每种操作，判断其是`Put`还是`Delete`，对于`Put`操作，调用 `engine_util.PutCF`实现；对于`Delete`操作，调用`engine_util.DeleteCF`实现。

### part2：实现原始键值服务处理程序

文档要求使用实现的存储引擎构建原始键值服务处理程序，就是对存储引擎的再一次封装。具体地，我们需要在`raw_api.go`中，实现`RawGet`，`RawScan`，`RawPut`，`RawDelete`四个方法。

对于`RawGet`方法，该方法传入一个`*kvrpcpb.RawGetRequest`类型的参数，代表`RawGet`请求。函数需要返回一个`*kvrpcpb.RawGetResponse` 类型的响应。

``` go
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error)
```

在`kvrpcpb.pb.go`中可以查看这两种类型的定义。

```go
// Raw commands.
type RawGetRequest struct {
	Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
	Key                  []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Cf                   string   `protobuf:"bytes,3,opt,name=cf,proto3" json:"cf,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
type RawGetResponse struct {
	RegionError *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
	Error       string         `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
	Value       []byte         `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
	// True if the requested key doesn't exist; another error will not be signalled.
	NotFound             bool     `protobuf:"varint,4,opt,name=not_found,json=notFound,proto3" json:"not_found,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
```

我们需要的字段就是 `RawGetRequest`中的`Key`和`Cf`。并根据`Key`和`Cf`,使用`reader`的`GetCF`方法获取相应`value` 值，然后创建一个`RawGetResponse`响应对象，包含获取到的值`value`和一个布尔值`NotFound`，表示是否找到了该键。最后返回`RawGetResponse`响应对象即可。

```go
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	} 
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	rawGetResponse := &kvrpcpb.RawGetResponse{
		Value : value,
		NotFound : (value == nil),
	}
	return rawGetResponse, nil
}
```

对于`RawPut`，`RawDelete`，`RawScan`方法也是同理，在`kvrpcpb.pb.go`中查看相应类型的定义，使用相应方法获取指定的值，根据相应的值创建相应的响应对象返回即可。

```go
// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := storage.Modify {
		Data : storage.Put{ Key: req.Key, Value: req.Value, Cf: req.Cf },
	}
	err := server.storage.Write(req.Context,[]storage.Modify{batch})
	if err != nil {
		return nil,err
	}

	rawPutResponse := &kvrpcpb.RawPutResponse{}
	return rawPutResponse, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := storage.Modify {
		Data : storage.Delete{ Key: req.Key, Cf: req.Cf }, 
	}
	err := server.storage.Write(req.Context,[]storage.Modify{batch})
	if err != nil {
		return nil,err
	}
	rawDeleteResponse := &kvrpcpb.RawDeleteResponse{}
	return rawDeleteResponse, nil
}
// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil,err
	}


	var kvs []*kvrpcpb.KvPair

	it := reader.IterCF(req.Cf)
	defer it.Close()

	limit := req.Limit

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()

		value, _ := item.Value()

		cur := &kvrpcpb.KvPair{
			Key : item.Key(),
			Value : value,
		}

		kvs = append(kvs,cur)
		limit = limit - 1

		if limit == 0 {
			break
		}
	}

	rawScanResponse := &kvrpcpb.RawScanResponse{
		Kvs : kvs,
	}
	return rawScanResponse, nil
}
```

需要注意`*kvrpcpb.RawScanRequest`类型中有`Limit`字段，其限制了一次扫描操作的最大键值对数量，这对于避免一次性读取过多数据、控制内存使用和提高性能非常有用。所以在实现`RawScan`方法时要注意维护当前扫描的数量。

```go
type RawScanRequest struct {
	Context  *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
	StartKey []byte   `protobuf:"bytes,2,opt,name=start_key,json=startKey,proto3" json:"start_key,omitempty"`
	// The maximum number of values read.
	Limit                uint32   `protobuf:"varint,3,opt,name=limit,proto3" json:"limit,omitempty"`
	Cf                   string   `protobuf:"bytes,4,opt,name=cf,proto3" json:"cf,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}
```

## 1.4 项目通过记录

![](/imgsrc/3.png)



# 2 Project2 RaftKV

## 2.1 项目指引文档

Raft 是一种设计为易于理解的共识算法。你可以在 Raft 网站上阅读有关 Raft 本身的资料，浏览 Raft 的交互式可视化，以及其他资源，包括扩展版的 Raft 论文。

在这个项目中，你将基于 Raft 实现一个高可用的键值服务器，这不仅需要你实现 Raft 算法，还需要实际使用它，并带来更多挑战，如使用 Badger 管理 Raft 的持久化状态、为快照消息添加流量控制等。

该项目包括三个部分：

1. 实现基本的 Raft 算法
2. 在 Raft 上构建一个容错的键值服务器
3. 增加 Raft 日志 GC 和快照支持

## Part A

### 代码

在这一部分，你将实现基本的 Raft 算法。需要实现的代码在 raft/ 目录下。raft/ 目录中有一些骨架代码和测试用例等待你去实现。你将要在这里实现的 Raft 算法具有与上层应用程序的良好接口设计。此外，它使用一个逻辑时钟（在这里称为 tick）来测量选举和心跳超时，而不是物理时钟。也就是说，不要在 Raft 模块本身设置定时器，上层应用程序负责通过调用 RawNode.Tick() 来推进逻辑时钟。除此之外，消息发送和接收以及其他事情都是异步处理的，何时实际执行这些操作也取决于上层应用程序（详见下文）。例如，Raft 不会阻塞等待任何请求消息的响应。

在实现之前，请首先查看本部分的提示。你还应该大致了解一下 proto 文件 proto/proto/eraftpb.proto。Raft 发送和接收的消息及相关结构都定义在这里，你将在实现中使用它们。注意，与 Raft 论文不同，它将心跳和附加条目消息分为不同的消息，以使逻辑更加清晰。

这一部分可以分为三个步骤：

1. 领导者选举
2. 日志复制
3. 原始节点接口

### 实现 Raft 算法

raft.Raft 在 raft/raft.go 中提供了 Raft 算法的核心，包括消息处理、驱动逻辑时钟等。有关更多的实现指南，请查看 raft/doc.go，其中包含了设计概述和这些消息类型的职责。

#### 领导者选举

要实现领导者选举，可以从 raft.Raft.tick() 开始，该函数用于将内部逻辑时钟推进一个 tick，从而驱动选举超时或心跳超时。现在你不需要关心消息的发送和接收逻辑。如果需要发送消息，只需将其推入 raft.Raft.msgs，所有接收到的消息将通过 raft.Raft.Step() 传递。测试代码将从 raft.Raft.msgs 获取消息，并通过 raft.Raft.Step() 传递响应消息。raft.Raft.Step() 是消息处理的入口，你需要处理像 MsgRequestVote、MsgHeartbeat 及其响应的消息。同时，请实现并正确调用测试存根函数，如 raft.Raft.becomeXXX，该函数用于在 raft 的角色变化时更新其内部状态。

你可以运行 make project2aa 来测试实现，并在本部分末尾查看一些提示。

#### 日志复制

要实现日志复制，可以从处理 MsgAppend 和 MsgAppendResponse 消息开始，既包括发送方也包括接收方。查看 raft/log.go 中的 raft.RaftLog，它是一个帮助管理 raft 日志的辅助结构，你还需要通过 raft/storage.go 中定义的 Storage 接口与上层应用程序交互，以获取持久化数据，如日志条目和快照。

你可以运行 make project2ab 来测试实现，并在本部分末尾查看一些提示。

#### 实现原始节点接口

raft/rawnode.go 中的 raft.RawNode 是我们与上层应用程序交互的接口，raft.RawNode 包含 raft.Raft 并提供一些包装函数，如 RawNode.Tick() 和 RawNode.Step()。它还提供 RawNode.Propose() 以让上层应用程序提出新的 Raft 日志。

另一个重要的结构 Ready 也在这里定义。在处理消息或推进逻辑时钟时，raft.Raft 可能需要与上层应用程序交互，如：

- 向其他节点发送消息
- 将日志条目保存到稳定存储
- 将硬状态（如 term、commit index 和 vote）保存到稳定存储
- 将提交的日志条目应用到状态机
- 等等

但这些交互不会立即发生，而是封装在 Ready 中并由 RawNode.Ready() 返回给上层应用程序。何时调用 RawNode.Ready() 并处理它由上层应用程序决定。在处理返回的 Ready 之后，上层应用程序还需要调用一些函数，如 RawNode.Advance()，以更新 raft.Raft 的内部状态，如已应用的索引、稳定的日志索引等。

你可以运行 make project2ac 来测试实现，并运行 make project2a 来测试整个 Part A。

### 提示：

- 在 raft.Raft、raft.RaftLog、raft.RawNode 和 eraftpb.proto 上添加任何需要的状态
- 测试假定首次启动 Raft 时应为 term 0
- 测试假定新选出的领导者应在其任期内附加一个 noop 条目
- 测试假定领导者一旦推进其提交索引，将通过 MessageType_MsgAppend 消息广播提交索引
- 测试未为本地消息 MessageType_MsgHup、MessageType_MsgBeat 和 MessageType_MsgPropose 设置 term
- 在领导者和非领导者之间附加日志条目有很大不同，有不同的来源、检查和处理，要小心
- 不要忘记不同节点之间的选举超时应不同
- rawnode.go 中的一些包装函数可以通过 raft.Step（本地消息）实现
- 启动一个新的 Raft 时，从 Storage 获取最后的稳定状态以初始化 raft.Raft 和 raft.RaftLog

## Part B

在这部分中，你将使用在 Part A 中实现的 Raft 模块构建一个容错的键值存储服务。你的键值服务将是一个复制状态机，由多个使用 Raft 进行复制的键值服务器组成。即使在发生故障或网络分区时，只要大多数服务器存活并能够通信，你的键值服务应继续处理客户端请求。

在 Project1 中，你已经实现了一个独立的键值服务器，因此你应该已经熟悉了键值服务器的 API 和存储接口。

在介绍代码之前，你需要首先了解三个术语：Store、Peer 和 Region，这些术语在 proto/proto/metapb.proto 中定义。

- **Store** 代表一个 tinykv-server 实例
- **Peer** 代表在 Store 上运行的一个 Raft 节点
- **Region** 是 Peers 的集合，也称为 Raft 组

为简单起见，在 Project2 中每个 Store 上只有一个 Peer，并且在集群中只有一个 Region。因此，你现在不需要考虑 Region 的范围。多个 Region 将在 Project3 中进一步介绍。

### 代码

首先，你应该看看 kv/storage/raft_storage/raft_server.go 中的 RaftStorage，它也实现了 Storage 接口。与直接对底层引擎进行读写的 StandaloneStorage 不同，RaftStorage 首先将每个读写请求发送给 Raft，然后在 Raft 提交请求后对底层引擎进行实际读写。通过这种方式，它可以保持多个 Store 之间的一致性。

RaftStorage 创建了一个 Raftstore 来驱动 Raft。当调用 Reader 或 Write 函数时，它实际上是通过通道（raftWorker 的 raftCh 通道）向 raftstore 发送定义在 proto/proto/raft_cmdpb.proto 中的 RaftCmdRequest（包含四种基本命令类型：Get/Put/Delete/Snap），并在 Raft 提交并应用命令后返回响应。Reader 和 Write 函数的 kvrpc.Context 参数现在很有用，它从客户端的角度携带 Region 信息，并作为 RaftCmdRequest 的头部传递。这些信息可能不正确或过时，因此 raftstore 需要检查它们并决定是否提出请求。

接下来是 TinyKV 的核心——raftstore。这个结构有点复杂，请阅读 TiKV 参考资料以更好地理解设计：

- [TiKV 多 Raft 设计与实现（中文）](https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore)
- [Design and Implementation of Multi-Raft (English Version)](https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore)

raftstore 的入口是 Raftstore，见 kv/raftstore/raftstore.go。它启动了一些异步处理特定任务的 worker，其中大部分现在不使用，所以你可以忽略它们。你只需要关注 raftWorker。（kv/raftstore/raft_worker.go）

整个过程分为两部分：raft worker 轮询 raftCh 获取消息，包括驱动 Raft 模块的基本 tick 和作为 Raft 条目提出的 Raft 命令；它从 Raft 模块获取并处理 ready，包括发送 raft 消息，持久化状态，将提交的条目应用于状态机。一旦应用，返回响应给客户端。

### 实现 Peer Storage

Peer Storage 是你通过 Part A 中的 Storage 接口进行交互的内容，但除了 Raft 日志之外，Peer Storage 还管理其他持久化的元数据，这对于在重启后恢复一致性状态机非常重要。此外，proto/proto/raft_serverpb.proto 中定义了三个重要状态：

- **RaftLocalState**：用于存储当前 Raft 的 HardState 和最后的日志索引。
- **RaftApplyState**：用于存储 Raft 应用的最后日志索引和一些截断的日志信息。
- **RegionLocalState**：用于存储 Region 信息和在此 Store 上对应的 Peer 状态。Normal 表示该 Peer 正常，Tombstone 表示该 Peer 已从 Region 中移除，不能加入 Raft 组。

这些状态存储在两个 badger 实例中：raftdb 和 kvdb：

- **raftdb** 存储 Raft 日志和 RaftLocalState
- **kvdb** 存储不同列族中的键值数据、RegionLocalState 和 RaftApplyState。你可以将 kvdb 视为 Raft 论文中提到的状态机

格式如下，并且在 kv/raftstore/meta 中提供了一些辅助函数，可以使用 writebatch.SetMeta() 将它们设置到 badger 中。

| Key              | KeyFormat                        | Value            | DB   |
| ---------------- | -------------------------------- | ---------------- | ---- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

你可能会疑惑为什么 TinyKV 需要两个 badger 实例。实际上，它可以使用一个 badger 存储 Raft 日志和状态机数据。分成两个实例只是为了与 TiKV 设计保持一致。

这些元数据应在 PeerStorage 中创建和更新。创建 PeerStorage 时，参见 kv/raftstore/peer_storage.go。它初始化此 Peer 的 RaftLocalState 和 RaftApplyState，或在重启时从底层引擎获取先前的值。注意，RAFT_INIT_LOG_TERM 和 RAFT_INIT_LOG_INDEX 的值是 5（只要大于 1 即可）而不是 0。原因是为了与在 conf change 后被动创建的 peer 区分开来。你现在可能不太理解，所以只需记住这一点，详细内容将在 Project3b 中实现 conf change 时描述。

你在这一部分需要实现的代码只有一个函数：PeerStorage.SaveReadyState，这个函数的作用是将 raft.Ready 中的数据保存到 badger 中，包括附加日志条目和保存 Raft 硬状态。

- **附加日志条目**：只需将 raft.Ready.Entries 中的所有日志条目保存到 raftdb，并删除任何先前附加但不会被提交的日志条目。同时，更新 peer storage 的 RaftLocalState 并保存到 raftdb。
- **保存硬状态**：也非常简单，只需更新 peer storage 的 RaftLocalState.HardState 并保存到 raftdb。

### 提示：

- 使用 WriteBatch 一次性保存这些状态。
- 查看 peer_storage.go 中的其他函数以了解如何读写这些状态。
- 设置环境变量 LOG_LEVEL=debug，可能会帮助你调试，参见所有可用的日志级别。

### 实现 Raft Ready 处理

在 Project2 Part A 中，你已经构建了一个基于 tick 的 Raft 模块。现在你需要编写外部进程来驱动它。大部分代码已经在 kv/raftstore/peer_msg_handler.go 和 kv/raftstore/peer.go 下实现。因此，你需要学习这些代码并完成 proposeRaftCommand 和 HandleRaftReady 的逻辑。以下是框架的一些解释。

Raft RawNode 已经通过 PeerStorage 创建并存储在 peer 中。在 raft worker 中，你可以看到它获取 peer 并由 peerMsgHandler 包装。peerMsgHandler 主要有两个函数：一个是 HandleMsg，另一个是 HandleRaftReady。

- **HandleMsg** 处理从 raftCh 接收到的所有消息，包括调用 RawNode.Tick() 以驱动 Raft 的 MsgTypeTick、包装客户端请求的 MsgTypeRaftCmd 以及在 Raft 节点之间传输的消息 MsgTypeRaftMessage。所有消息类型都定义在 kv/raftstore/message/msg.go 中。你可以查看详细信息，其中一些将在后续部分使用。
- 消息处理完后，Raft 节点应有一些状态更新。因此，HandleRaftReady 应从 Raft 模块获取 ready 并执行相应的操作，如持久化日志条目、应用提交的条目以及通过网络向其他节点发送 raft 消息。

伪代码中，raftstore 使用 Raft 如下：

```
go复制代码for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```

完成这部分后，一个读写操作的完整过程如下：

1. 客户端调用 RPC RawGet/RawPut/RawDelete/RawScan
2. RPC 处理程序调用 RaftStorage 相关方法
3. RaftStorage 通过 raftstore 发送 Raft 命令请求，并等待响应
4. RaftStore 将 Raft 命令请求作为 Raft 日志提出
5. Raft 模块附加日志，并由 PeerStorage 持久化
6. Raft 模块提交日志
7. Raft worker 在处理 Raft ready 时执行 Raft 命令，并通过回调返回响应
8. RaftStorage 从回调接收响应并返回给 RPC 处理程序
9. RPC 处理程序执行一些操作并将 RPC 响应返回给客户端。

你应该运行 `make project2b` 以通过所有测试。整个测试是运行一个包括多个 TinyKV 实例的模拟集群与一个模拟网络。它执行一些读写操作并检查返回值是否符合预期。

需要注意的是，错误处理是通过测试的一个重要部分。你可能已经注意到，在 proto/proto/errorpb.proto 中定义了一些错误，这些错误是 gRPC 响应的一个字段。此外，kv/raftstore/util/error.go 中定义了实现 error 接口的相应错误，因此你可以使用它们作为函数的返回值。

这些错误主要与 Region 相关。因此，它也是 RaftCmdResponse 的 RaftResponseHeader 的一个成员。在提出请求或应用命令时，可能会出现一些错误。如果出现错误，你应该返回包含错误的 Raft 命令响应，然后错误将进一步传递到 gRPC 响应中。你可以使用 kv/raftstore/cmd_resp.go 中提供的 BindRespError 在返回包含错误的响应时将这些错误转换为 errorpb.proto 中定义的错误。

在这个阶段，你可以考虑这些错误，其他错误将在 Project3 中处理：

- **ErrNotLeader**：Raft 命令在跟随者上提出，因此使用它让客户端尝试其他节点。
- **ErrStaleCommand**：可能由于领导者更改，一些日志没有提交并被新领导者的日志覆盖。但客户端不知道这一点，仍在等待响应。因此，你应该返回此错误，让客户端知道并重新尝试命令。

### 提示：

- PeerStorage 实现了 Raft 模块的 Storage 接口，你应该使用提供的方法 SaveReadyState() 来持久化 Raft 相关状态。
- 使用 engine_util 中的 WriteBatch 以原子方式进行多次写入，例如，你需要确保在一个写入批处理中应用提交的条目并更新应用索引。
- 使用 GlobalContext 中的 Transport 向其他节点发送 raft 消息。
- 服务器不应在不属于多数且没有最新数据时完成 get RPC。你可以将 get 操作放入 raft 日志中，或实现 Raft 论文第 8 节中描述的只读操作优化。
- 应用日志条目时，不要忘记更新并持久化应用状态。
- 你可以以异步方式应用提交的 Raft 日志条目，就像 TiKV 那样。这不是必须的，但对提高性能是一个大的挑战。
- 提出命令时记录回调，并在应用后返回回调。
- 对于 snap 命令响应，应显式设置 badger Txn 到回调。
- 在 2A 之后，有些测试可能需要运行多次才能发现错误。

`/raft/doc.go`：

```go
// 版权所有 2015 The etcd Authors
//
// 根据 Apache 许可证 2.0 版（以下简称“许可证”）获得许可；
// 除非遵守许可证，否则您不得使用此文件。
// 您可以在以下地址获取许可证副本：
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// 除非适用法律要求或书面同意，根据许可证分发的软件
// 按“原样”分发，不附带任何明示或暗示的担保或条件。
// 有关许可证下权限和限制的具体语言，请参阅许可证。

/*
raft 包发送和接收 eraftpb 包中定义的协议缓冲区格式的消息。

Raft 是一种协议，通过该协议可以使一组节点维护一个复制状态机。
状态机通过使用复制日志保持同步。
有关 Raft 的更多详细信息，请参阅 "In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf) 作者 Diego Ongaro 和 John Ousterhout。

使用方法

raft 的主要对象是一个 Node。你可以通过 raft.StartNode 从头开始启动一个节点
或者通过 raft.RestartNode 从某个初始状态启动一个节点。

从头开始启动节点：

  storage := raft.NewMemoryStorage()
  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
  }
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从先前的状态重启节点：

  storage := raft.NewMemoryStorage()

  // 从持久化存储中恢复内存存储
  storage.ApplySnapshot(snapshot)
  storage.SetHardState(state)
  storage.Append(entries)

  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxInflightMsgs: 256,
  }

  // 重启 raft 无需 peer 信息。
  // peer 信息已包含在存储中。
  n := raft.RestartNode(c)

既然你已经持有一个 Node，你有一些责任：

首先，你必须读取 Node.Ready() 通道并处理其中的更新
这些步骤可以并行执行，除第 2 步外。

1. 将 HardState、Entries 和 Snapshot 写入持久化存储（如果它们不为空）。注意，当写入一个索引为 i 的条目时，
必须丢弃任何以前持久化的索引 >= i 的条目。

2. 将所有消息发送到消息中命名的节点。重要的是，在最新的 HardState 持久化到磁盘之前，
以及任何之前的 Ready 批次写入的所有条目之前，不能发送任何消息（在持久化来自同一批次的条目时可以发送消息）。

注意：消息的序列化不是线程安全的；重要的是确保在序列化过程中不会有新的条目被持久化。
最简单的方法是在你的主要 raft 循环内直接序列化消息。

3. 将 Snapshot（如果有）和 CommittedEntries 应用到状态机。
如果任何提交的条目类型是 EntryType_EntryConfChange，调用 Node.ApplyConfChange()
将其应用到节点。如果此时取消配置更改，可以在调用 ApplyConfChange 之前将 NodeId 字段设置为零
（但必须调用 ApplyConfChange，无论哪种方式，并且取消的决定必须完全基于状态机，而不是外部信息，如
观察到的节点健康状况）。

4. 调用 Node.Advance() 以表示准备好接收下一批更新。
在第 1 步之后的任何时候都可以进行，尽管所有更新必须按照 Ready 返回的顺序处理。

其次，所有持久化的日志条目必须通过 Storage 接口的实现提供。提供的 MemoryStorage
类型可以用于此（如果你在重启时重新填充其状态），或者你可以提供自己的基于磁盘的实现。

第三，当你从其他节点接收到消息时，将其传递给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，你需要定期调用 Node.Tick()（可能通过 time.Ticker）。Raft 有两个重要的超时：心跳和
选举超时。然而，在 raft 包内部时间由抽象的“tick”表示。

完整的状态机处理循环看起来像这样：

  for {
    select {
    case <-s.Ticker:
      n.Tick()
    case rd := <-s.Node.Ready():
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == eraftpb.EntryType_EntryConfChange {
          var cc eraftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      s.Node.Advance()
    case <-s.done:
      return
    }
  }

要从你的节点向状态机提出变更，将你的应用程序数据序列化为一个字节切片并调用：

	n.Propose(data)

如果提案被提交，数据将以 eraftpb.EntryType_EntryNormal 类型出现在提交的条目中。没有保证提议的命令会被
提交；在超时后你可能需要重新提议。

要在集群中添加或删除节点，构建 ConfChange 结构 'cc' 并调用：

	n.ProposeConfChange(cc)

在配置更改提交后，将返回类型为 eraftpb.EntryType_EntryConfChange 的某些提交条目。你必须通过以下方式将其应用到节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 表示集群中的唯一节点。一个
给定的 ID 必须只使用一次，即使旧节点已被删除。
这意味着例如 IP 地址作为节点 ID 很差，因为它们
可能被重用。节点 ID 必须非零。

实现注意事项

该实现与最终的 Raft 论文（https://ramcloud.stanford.edu/~ongaro/thesis.pdf）保持一致，尽管我们的
成员变更协议的实现与
第 4 章描述的有所不同。关键的不变性是成员变更
一次只发生一个节点，但在我们的实现中，成员变更在其条目应用时生效，而不是在其
被添加到日志时生效（因此条目在旧
成员配置下提交而不是新配置下提交）。这在安全性方面是等效的，
因为旧配置和新配置保证重叠。

为了确保我们不会通过匹配日志位置尝试提交两个成员变更
（这将是不安全的，因为它们
应该有不同的仲裁要求），我们简单地不允许任何
提议的成员变更出现在
领导者的日志中有任何未提交的变更时。

这种方法在尝试从两成员集群中移除成员时引入了一个问题：如果
在另一个成员收到 confchange 条目提交之前一个成员死亡，则成员
不能再被移除，因为集群不能再进展。
因此强烈建议在
每个集群中使用三个或更多节点。

消息类型

Raft 包发送和接收 eraftpb 包中定义的协议缓冲区格式的消息。每个状态（follower、candidate、leader）实现了其
自己的 'step' 方法（'stepFollower'、'stepCandidate'、'stepLeader'）来
推进给定的 eraftpb.Message。每个步骤由其
eraftpb.MessageType 决定。注意每个步骤都由一个通用方法
'Step' 检查，以安全检查节点和传入消息的术语以防止
陈旧的日志条目：

	'MessageType_MsgHup' 用于选举。如果节点是 follower 或 candidate，
	'raft' 结构中的 'tick' 函数设置为 'tickElection'。如果 follower 或
	candidate 在选举超时之前没有收到任何心跳，
	它会将 'MessageType_MsgHup' 传递给其 Step 方法并变为（或保持）候选人以
	启动新的选举。

	'MessageType_MsgBeat' 是一个内部类型，信号领导者发送心跳
	'MessageType_MsgHeartbeat' 类型的心跳。如果节点是领导者，
	'raft' 结构中的 'tick' 函数设置为 'tickHeartbeat'，并触发领导者
	向其 follower 周期性发送 'MessageType_MsgHeartbeat' 消息。

	'MessageType_MsgPropose' 提议将数据附加到其日志条目中。这是一个特殊
	类型，将提议重定向到领导者。因此，send 方法会覆盖
	eraftpb.Message 的术语与其 HardState 的术语，以避免将其
	本地术语附加到 'MessageType_MsgPropose'。当 'MessageType_MsgPropose'
	被发送到其 Step 方法时，候选人会返回
	'MessageType_MsgPropose' 的 'ErrProposalDropped' 错误。

	'MessageType_MsgAppend' 包含要复制的日志条目。如果它来自合法的领导者，
	接收者将存储这些条目并回复 'MessageType_MsgAppendResponse'，
	如果它们的术语与日志匹配则返回 'ErrSuccess'，否则返回
	'ErrCompacted' 或 'ErrSnapshot'.

	'MessageType_MsgRequestVote' 包含请求者的最新日志位置。
	如果接收者的日志匹配（术语和索引大于或等于
		请求者的日志）并且它未投票或投票给请求者，
	则接收者将回复 'MessageType_MsgRequestVoteResponse' 并将其 vote 字段设置为请求者 ID。

	'MessageType_MsgSnapshot' 请求接收者将其状态机恢复到快照的状态。
	接收者将回复 'MessageType_MsgAppendResponse' 并将其 context 字段设置为 'snappy'（快照
	数据的压缩格式）。

	'MessageType_MsgHeartbeat' 是领导者发送的，信号 follower
	该领导者仍处于活动状态。如果接收者不是 follower，则
	会将其 term 递增并转发给候选人。

	'MessageType_MsgHeartbeatResponse' 是 follower 回复给领导者
	响应 'MessageType_MsgHeartbeat' 的。如果接收者不是 follower，它会
	忽略此消息。如果接收者是 follower，则会
	重置其心跳超时并转发给其领导者。
*/

```

