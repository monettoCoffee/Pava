# What is this
- This is a utility library that does not require any external dependencies and is written entirely using Python's native libraries.
- I was originally a Java developer, so this library has a slightly Java-like style overall.
- The name of this library is inspired by a Java library called Guava.

- 这是一个不需要任何外部依赖的，全部使用 Python 原生库编写的工具库。
- 我最初是一名 Java 开发工作者，所以这个库在整体上风格偏向 Java 一些。
- 这个名字的灵感来源于一个 Java 库，名字叫 Guava。

# What can it do
## Simple message queue
- The source code is located in the component/mq directory.
- The design utilizes the built-in SQLite library in Python for message persistence. SQL statements are used to manipulate the status of a message, such as "being consumed" or "consumption failed."
- To improve concurrency, the design adopts a sharding approach where different topics are stored in different SQLite files. This allows SQLite to overcome some of the limitations of single-threaded writes.
- The message management module uses a separate SQLite file and ensures eventual consistency between the management module and the storage module through SQLite transactions.
- Users can directly consume or delete messages in the management module.

## 简易的消息队列：
- 源码位于 component/mq 目录下。
- 设计思想是采用 Python 自带的 sqlite 去做消息的持久化，通过 SQL 语句去操作一个消息的状态，例如 消费中，消费失败。
- 同时，为了提高并发度，采用了 分库分文件 的设计思路，对于不同的 Topic，存放于不同的 SQLite 文件中。使得 SQLite 可以克服部分写入单线程的问题。
- 消息管理模块，则单独采用了一个独立的 SQLite 文件，采用 SQLite 事务的方式，保证管理模块和存储模块的最终一致性。
- 用户也可以在管理模块中直接对一些消息进行直接消费，或者删除。

## Simple task scheduler
- The code for the scheduler is located in the component/scheduler directory.
- The scheduler supports both delayed execution of tasks and periodic execution of tasks.
- The design utilizes a combination of timed sleep and task encapsulation. The scheduler is awakened every second to check for any pending tasks. If there are tasks to be executed, they are placed in a thread pool for execution.
- If a task needs to be executed periodically, it is re-registered in the execution queue.

## 简易任务调度模块
- 源码位于 component/scheduler 目录下。
- 支持 延时执行一个任务，与 定期执行一个任务。
- 设计思想是采用了 定时 Sleep + 封装 Task，每秒钟调度器会被唤醒一次，检查是否有需要执行的任务，如果有的话，那么放入到执行的线程池中进行运行。
- 如果这个任务需要周期性运行，那么会将这个任务再次注册到执行队列中。

## Python-Synchronized
- The code for the synchronized decorator can be found in the decorator_impl/synchronized_decorator file.
- The design utilizes the built-in Lock in Python along with a decorator to achieve a similar effect to Java's `synchronized`. It provides a simple way to implement synchronization without needing to focus on the details. Just applying the decorator is enough to achieve the desired synchronization effect.
- It supports reentrant locking, meaning that the same thread can enter the synchronized block multiple times. It also allows for flexible switching based on the lock key.
- In the simple message queue implementation, the `@synchronized` decorator is used as a fallback to ensure single-threaded writes in SQLite.

- 源码位于 decorator_impl/synchronized_decorator 文件下。
- 设计的思想是采用 Python 自带的 Lock + 装饰器 起到一个类似于 Java Synchronized 的效果，不需要关注过多的细节，仅需要一个装饰器就可以实现同步锁的效果。
- 支持同一个线程可冲入，并且可以根据 锁 Key 进行灵活切换。
- 在 简易消息队列 中，就采用了 @synchronized 作为 SQLite 单线程写入的最终兜底。

## Python-TypeCheck
- The code for the type check decorator can be found in the decorator_impl/type_check_decorator file.
- The main objective is to address the issue of unexpected behavior and difficulty in debugging when encountering unexpected types of parameters after running Python code.
- The design utilizes Python decorators to achieve a non-intrusive effect on business logic.
- During runtime, it checks whether the types of the parameters passed into a function match the expected configuration. If they do not match, an exception is raised immediately, terminating the execution.

- 源码位于 decorator_impl/type_check_decorator 文件下。
- 主要想解决，当 Python 运行后，面对非预期类型的参数，可能出现非预期行为，造成查日志调错困难。
- 设计的思想是采用 Python 的装饰器，达到一个对业务无入侵的效果。
- 在运行时，会检查 传入函数中的参数类型，是否与期望配置的参数类型相符合，如果不符合，则及时抛出异常，终止运行。