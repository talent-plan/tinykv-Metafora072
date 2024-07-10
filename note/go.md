# 1 Go语言基础

## 1.1 变量

Go 语言变量名由字母、数字、下划线组成，其中首个字符不能为数字。

声明变量的一般形式是使用 var 关键字：

```go
var identifier type
```

可以一次声明多个变量：

```go
var identifier1, identifier2 type
```

声明变量并将其初始化有三种方案：

1. 使用`var`声明变量及其类型，后对变量进行赋值实现初始化：

```go
var v_name v_type
v_name = value
```

2. 根据值自行判定变量类型

```go
var v_name = value
```

3. 使用`:=` 运算符，声明变量的同时进行初始化：

```go
v_name := value
```

如果变量已经使用 var 声明过了，再使用 `:=`声明变量，就产生编译错误：

```go
var intVal int 
intVal := 1 // 这时候会产生编译错误，因为 intVal 已经声明，不需要重新声明
```

Go语言允许多变量声明，如下：

```go
//类型相同多个变量, 非全局变量
var vname1, vname2, vname3 type
vname1, vname2, vname3 = v1, v2, v3

var vname1, vname2, vname3 = v1, v2, v3 // 和 python 很像,不需要显示声明类型，自动推断

vname1, vname2, vname3 := v1, v2, v3 // 出现在 := 左侧的变量不应该是已经被声明过的，否则会导致编译错误


// 这种因式分解关键字的写法一般用于声明全局变量
var (
    vname1 v_type1
    vname2 v_type2
)
```

## 1.2 函数

Go 语言函数定义格式如下：

```go
func function_name( [parameter list] ) [return_types] {
   //函数体
}
```

- func：函数由 func 开始声明
- function_name：函数名称，参数列表和返回值类型构成了函数签名。
- parameter list：参数列表，参数就像一个占位符，当函数被调用时，你可以将值传递给参数，这个值被称为实际参数。参数列表指定的是参数类型、顺序、及参数个数。参数是可选的，也就是说函数也可以不包含参数。
- return_types：返回类型，函数返回一列值。return_types 是该列值的数据类型。有些功能不需要返回值，这种情况下 return_types 不是必须的。
- 函数体：函数定义的代码集合。

Go 函数可以返回多个值：

```go
package main

import "fmt"

func swap(x string,y string) (string, string) {
   return y, x
}

func main() {
   a, b := swap("X", "Y")
   fmt.Println(a, b)
}

// print: Y X
```

## 1.3 数组

Go 语言数组声明需要指定元素类型及元素个数，语法格式如下：

```go
var arrayName [size]dataType
```

其中，**arrayName** 是数组的名称，**size** 是数组的大小，**dataType** 是数组中元素的数据类型。

在声明时，数组中的每个元素都会根据其数据类型进行默认初始化，对于整数类型，初始值为 0。

```go
var numbers [5]int
```

还可以使用初始化列表来初始化数组的元素：

```go
var numbers = [5]int{1, 2, 3, 4, 5}
```

我们也可以通过字面量在声明数组的同时快速初始化数组：

```go
balance := [5]float32{1000.0, 2.0, 3.4, 7.0, 50.0}
```

如果数组长度不确定，可以使用 **...** 代替数组的长度，编译器会根据元素个数自行推断数组的长度：

```go
var balance = [...]float32{1000.0, 2.0, 3.4, 7.0, 50.0}
或
balance := [...]float32{1000.0, 2.0, 3.4, 7.0, 50.0}
```

如果设置了数组的长度，我们还可以通过指定下标来初始化元素：

```go
//  将索引为 1 和 3 的元素初始化
balance := [5]float32{1:2.0,3:7.0}
```

初始化数组中 **{}** 中的元素个数不能大于 **[]** 中的数字。

如果忽略 **[]** 中的数字不设置数组大小，Go 语言会根据元素的个数来设置数组的大小：

```go
 balance[4] = 50.0
```

## 1.4 指针

类似于变量和常量，在使用指针前你需要声明指针。指针声明格式如下：

```go
var var_name *var-type
```

var-type 为指针类型，var_name 为指针变量名，* 号用于指定变量是作为一个指针。以下是有效的指针声明：

```go
var ip *int        /* 指向整型*/
var fp *float32    /* 指向浮点型 */
```

本例中这是一个指向 int 和 float32 的指针。

当一个指针被定义后没有分配到任何变量时，它的值为 nil，nil 指针也称为空指针。

## 1.5 结构体

结构体定义需要使用 type 和 struct 语句。struct 语句定义一个新的数据类型，结构体中有一个或多个成员。type 语句设定了结构体的名称。结构体的格式如下：

```go
type struct_variable_type struct {
   member definition
   member definition
   ...
   member definition
}
```

一旦定义了结构体类型，它就能用于变量的声明，语法格式如下：

```go
variable_name := structure_variable_type {value1, value2...valuen}
或
variable_name := structure_variable_type { key1: value1, key2: value2..., keyn: valuen}
```

如果要访问结构体成员，需要使用点号 **.** 操作符，格式为：

```go
结构体.成员名
```

你可以定义指向结构体的指针类似于其他指针变量，格式如下：

```go
var struct_pointer *Books
```

以上定义的指针变量可以存储结构体变量的地址。查看结构体变量地址，可以将 & 符号放置于结构体变量前：

```go
struct_pointer = &Book1
```

使用结构体指针访问结构体成员，使用 "." 操作符（与C语言的“->”不同，Go语言使用结构体指针访问结构体成员也是用“.”运算符）：

```go
struct_pointer.title
```

示例如下：

```go
package main

import "fmt"

type Books struct {
   title string
   author string
   subject string
   book_id int
}

func main(){
   book := Books{
      title : "ABC",
      author : "Bob",
      subject : "Math",
      book_id : 1001,
   }
   var ptr *Books = &book;
   ptr.title = "md";
   fmt.Println(ptr.title)
   var book2 = book;
   fmt.Println(book2.title)
}
// md 
// md
```

## 1.6 切片

### 1.6.1 切片的定义

切片是基于数组的引用类型，包含三个部分：指向数组的指针、切片的长度（len）和容量（cap）。切片的声明方式如下：

```go
var s []int
```

或者使用内置函数 `make` 来创建切片：

```go
s := make([]int, len, cap)
```

其中，`len` 是切片的长度，`cap` 是切片的容量。

### 1.6.2 切片的初始化

切片可以通过多种方式初始化：

- 直接初始化：

  ```go
  s := []int{1, 2, 3}
  ```

- 使用**make**函数：

  ```go
  s := make([]int, 5, 10)
  ```

- 从数组或其他切片中切取：

  ```go
  array := [5]int{1, 2, 3, 4, 5}
  s := array[1:4] // 包含索引1到3的元素
  ```

### 1.6.3 切片的长度和容量

- `len(s)` 返回切片的长度，即切片中元素的个数。
- `cap(s)` 返回切片的容量，即从切片的起始位置到底层数组末尾的元素个数。

### 1.6.4 切片的操作

- 追加元素：使用 **append** 函数可以向切片中追加元素。当切片的容量不足以容纳新元素时，会自动扩容。

  ```go
  s := []int{1, 2, 3}
  s = append(s, 4, 5)
  ```

- 复制切片：使用 **copy** 函数可以将一个切片的内容复制到另一个切片中。

  ```go
  s1 := []int{1, 2, 3}
  s2 := make([]int, len(s1))
  copy(s2, s1)
  ```

### 1.6.5 切片的扩容机制

当切片的容量不足时，`append` 函数会触发切片的扩容。扩容的规则如下：

- 如果新容量大于原容量的两倍，则新容量为所需容量。
- 如果原容量小于1024，则新容量为原容量的两倍。
- 如果原容量大于或等于1024，则新容量为原容量的1.25倍。

### 1.6.6 切片的共享底层数组

切片是对底层数组的引用，因此多个切片可以共享同一个底层数组。修改一个切片的元素会影响到共享同一底层数组的其他切片。

```go
array := [5]int{1, 2, 3, 4, 5}
s1 := array[1:4]
s2 := array[2:5]
s1[0] = 10 // 修改s1的第一个元素
fmt.Println(s2) // 输出：[10 3 4]
```

## 1.7 范围

Go 语言中 range 关键字用于 for 循环中迭代数组(array)、切片(slice)、通道(channel)或集合(map)的元素。在数组和切片中它返回元素的索引和索引对应的值，在集合中返回 key-value 对。

for 循环的 range 格式可以对 slice、map、数组、字符串等进行迭代循环。格式如下：

```go
for key, value := range oldMap {
    newMap[key] = value
}
```

如果只想读取 key，格式如下：

```go
for key := range oldMap
```

或者：

```go
for key, _ := range oldMap
```

如果只想读取 value，格式如下：

```go
for _, value := range oldMap
```