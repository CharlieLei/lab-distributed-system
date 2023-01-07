# Lab1 MapReduce

## 1. worker的工作 [mr/worker.go]

worker使用一个无限循环，不断通过RPC调用coordinator的函数AcquireTask来获得任务并执行；每当完成当前任务时，通过RPC调用FinishTask来告知coordinator已经完成该任务。 AcquireTask返回的任务包含4种类型：
- MapTask：worker获得map任务
- ReduceTask：worker获得reduce任务
- WaitTask：当前所有任务都已被coordinator分配出去，worker只需休眠一段时间
- FinishTask：所有任务已经完成，worker可以跳出循环

为了避免不同worker分配到同一任务时，在写输出文件时所产生的竞争，worker会先将结果写入一个临时文件，然后通过os.Rename来将临时文件的名字改成输出文件名。由于os.Rename是原子的，因此不同worker不会因为写同一个输出文件而发生竞争。

map任务读取输入文件，将其中的内容传入mapf函数，然后将函数返回结果按照key划分到nReduce个中间文件中，每个中间文件的名字为`mr-intermediate-X-Y`，其中X为map任务ID，Y为reduce编号

reduce任务读取所有含有自身reduce编号的中间文件，将其中的内容按照key聚合，然后将聚合结果传入reducef函数，最后将结果写入结果文件`mr-out-Y`


## 2. coordinator的工作 [mr/coordinator.go]

coordinator维护若干队列：
- 未分配的map任务
- 已分配正在运行的map任务
- 已完成的map任务
- 未分配的reduce任务
- 已分配正在运行的reduce任务
- 已完成的reduce任务

在给worker分配一个map或者reduce任务后，使用goroutine起一个定时器来监测该任务的执行情况，如果该任务超时（该任务在`正在运行队列`）且不在`已完成队列`中，则将该任务放回`未分配队列`中。

`已完成队列`是为了避免出现如下情况：
- 计时器时间短于执行某个任务的时间，因此......
  - 每次计时器超时会先将该任务放回`未分配队列`
  - 然后该任务分配给新的worker去执行，此时起了第二个计时器
  - 第一个worker完成任务
  - 第二个计时器超时，发现该任务仍在`正在运行队列`中，又将该任务放回`未分配队列`
  - 该任务就会一直被分配给worker执行
为了避免出现以下状况，计时器超时后，只有当该任务不在`已完成队列`时，才将该任务放回`未分配队列`中。

当coordinator收到某个worker完成的回复时，只可能是如下情况：
1. 该任务在`正在运行队列`中
    - 将该任务从`正在运行队列`中删除并加到`已完成队列`中
2. 该任务在`未分配队列`中
    - 将该任务从`未分配队列`中删除并加到`已完成队列`中
3. 该任务在`已完成队列`中
    - 直接将结果丢弃

coordinator通过map的`正在运行队列`和`未分配队列`来判断当前分配的是map任务还是reduce任务。因此每次收到worker返回map任务完成的回复时，coordinator需要检查这两个队列是否都为空，若全为空，则说明map任务已经全部完成，coordinator会对reduce的`未分配队列`进行初始化。

当coordinator中map的`正在运行队列`和`未分配队列`和reduce的`正在运行队列`和`未分配队列`全为空时，说明coordinator已经完成所有map和reduce任务
