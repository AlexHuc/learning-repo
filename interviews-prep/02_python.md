# 🧠 What is Python?
**`Python`** &rarr; a high-level, interpreted, general-purpose programming language known for its simplicity and readability

## 🧠 Is Python Case Sensitive?
✅ Yes, Python is case-sensitive

### 🔧 Example
```python
name = "Alice"
Name = "Bob"

print(name)  # Alice
print(Name)  # Bob
```

### ⚠️ Important Note
Even keywords are case-sensitive:
```python
True   # correct
true   # ❌ error
```

---

# `.py` vs `.pyc` files
## 🧠 What is a .py file?
**`.py`** &rarr; file is a Python source code file written by the developer

### 🎯 Characteristics
- human-readable
- contains Python code
- editable
- used to write programs

## 🧠 What is a .pyc file?
**`.pyc`** &rarr; file is a compiled version of a `.py` file, containing Python bytecode

### 🔧 What is bytecode?
**`bytecode`** &rarr; Intermediate code that Python executes (not machine code)

#### 📦 Where it appears
```shell
__pycache__/
   file.cpython-311.pyc
```

### 🎯 Characteristics
- not human-readable
- generated automatically
- improves performance
- used internally by Python

## ⚖️ .py vs .pyc
| Feature    | .py         | .pyc              |
| ---------- | ----------- | ----------------- |
| Type       | Source code | Compiled bytecode |
| Readable   | Yes         | No                |
| Editable   | Yes         | No                |
| Created by | Developer   | Python            |
| Purpose    | Write code  | Execute faster    |

---

# 🧠 How is Memory Managed in Python?
**`Memory Management`** &rarr; automatically using a combination of **reference counting** and **garbage collection**

## ⚙️ 1️⃣ Automatic Memory Management
👉 You don’t manually allocate/free memory like in C/C++
```python
x = 10
```
Python:
- allocates memory
- tracks it
- frees it when not needed

## 🧠 2️⃣ Reference Counting (MAIN MECHANISM)
**`Reference Counting`** &rarr; keeps track of how many references point to an object

### 🔧 Example
```python
a = [1, 2, 3]
b = a
```
👉 Now:
- a &rarr; object
- b &rarr; same object

Reference count = 2

#### 🔥 What happens next?
```python
del a
```
👉 Reference count = 1

```python
del b
```
👉 Reference count = 0 → object is deleted ✅

##### 🎯 Key Idea
- When reference count reaches 0, memory is freed

## 🧠 3️⃣ Garbage Collector (GC)
**`Garbage Collector`** &rarr; Reference counting cannot handle circular references

### 🔧 Example
```python
a = []
b = []
a.append(b)
b.append(a)
```
👉 Both reference each other &rarr; never reach 0 ❌

#### ✅ Solution
- Python uses a garbage collector to clean cyclic references

#### 🎯 How it works
- detects unreachable objects
- frees memory

## 🧠 4️⃣ Memory Pools & Allocator (Advanced)
**`Memory Pools & Allocator`** &rarr; uses a specialized memory manager:
- 👉 PyMalloc

### 🎯 Features
- efficient allocation for small objects
- reduces fragmentation
- uses memory pools

## 🧠 5️⃣ Stack vs Heap
### 🔹 Stack
- function calls
- local variables

### 🔹 Heap
- objects
- lists, dicts, etc.

👉 Python mainly manages memory in the heap

## ⚖️ Summary of Mechanisms
| Mechanism          | Purpose             |
| ------------------ | ------------------- |
| Reference counting | track object usage  |
| Garbage collector  | clean cycles        |
| PyMalloc           | optimize allocation |

---

# 🐍 Python Data Types Overview

| Category        | Data Type      | Description | Example | Mutable |
|----------------|---------------|------------|---------|---------|
| 🔢 Numeric      | int           | Integer numbers | 10 | ❌ No |
| 🔢 Numeric      | float         | Decimal numbers | 10.5 | ❌ No |
| 🔢 Numeric      | complex       | Complex numbers | 2 + 3j | ❌ No |
| 🔤 Text         | str           | String (text) | "hello" | ❌ No |
| 📦 Sequence     | list          | Ordered, mutable collection | [1, 2, 3] | ✅ Yes |
| 📦 Sequence     | tuple         | Ordered, immutable collection | (1, 2, 3) | ❌ No |
| 📦 Sequence     | range         | Sequence of numbers | range(5) | ❌ No |
| 🧾 Mapping      | dict          | Key-value pairs | {"a": 1} | ✅ Yes |
| 🔢 Set          | set           | Unordered, unique elements | {1, 2, 3} | ✅ Yes |
| 🔢 Set          | frozenset     | Immutable set | frozenset({1,2}) | ❌ No |
| 🧠 Boolean      | bool          | True/False values | True | ❌ No |
| 📦 Binary       | bytes         | Immutable binary data | b"abc" | ❌ No |
| 📦 Binary       | bytearray     | Mutable binary data | bytearray(5) | ✅ Yes |
| 📦 Binary       | memoryview    | Memory view of binary data | memoryview(b"abc") | Depends |
| 🚫 None Type    | NoneType      | Represents absence of value | None | ❌ No |

# 🔥 Mutable vs Immutable
## ✅ Mutable (can change)
- `list`
- `dict`
- `set`
- `bytearray`

## ❌ Immutable (cannot change)
- `int`
- `float`
- `str`
- `tuple`
- `frozenset`

---

# 🐍 Python: Array vs List vs Tuple vs Dictionary

| Feature        | Array | List | Tuple | Dictionary (dict) |
|----------------|------|------|-------|-------------------|
| 📦 Definition  | Collection of same-type elements | Collection of elements (any type) | Immutable collection of elements | Key-value pairs |
| 🔢 Data Type   | Same type only | Mixed types allowed | Mixed types allowed | Keys + values |
| 🧠 Example     | array('i', [1,2,3]) | [1, "a", 3.5] | (1, "a", 3.5) | {"name": "Alice"} |
| 🔄 Order       | Ordered | Ordered | Ordered | Ordered (Python 3.7+) |
| 🔁 Duplicates  | Allowed | Allowed | Allowed | Keys must be unique |
| ⚡ Access      | By index | By index | By index | By key |
| 🔧 Mutability  | Mutable | Mutable | ❌ Immutable | Mutable |
| 🧩 Use Case    | Numeric operations | General-purpose | Fixed data, safe data | Fast lookup by key |
| ⚙️ Built-in?   | No (needs module) | Yes | Yes | Yes |
| 🚀 Performance | Efficient for numbers | Flexible but slower | Faster than list (immutable) | Very fast lookup (O(1)) |

---

# 🧠 What is Comprehension?
**`Comprehension`** &rarr; a concise way to create collections (like lists or dictionaries) in one line

## 1️⃣ List Comprehension
**`List Comprehension`** &rarr; a compact way to create a list using a loop and optional condition

### 🔧 Syntax
```python
[expression for item in iterable if condition]
```

## 2️⃣ Dictionary Comprehension
**`Dictionary Comprehension`** &rarr; creates a dictionary in one line using key-value pairs

### 🔧 Syntax
```python
{key: value for item in iterable if condition}
```

---

# 🧠 What is Slicing in Python?
**`Slicing`** &rarr; a way to extract a portion (subset) of a sequence like a list, string, or tuple

## 🔧 Syntax
```python
sequence[start : end : step]
```

### 🎯 Meaning
- `start` &rarr; where to begin (inclusive)
- `end` &rarr; where to stop (exclusive)
- `step` &rarr; how many steps to move

---

## 🟢 Basic Examples
### 📦 Example List
```python
arr = [0, 1, 2, 3, 4, 5]
```

#### 1️⃣ Basic Slice
```python
arr[1:4]
# [1, 2, 3]
```

#### 2️⃣ From Start
```python
arr[:3]
# [0, 1, 2]
```

#### 3️⃣ Until End
```python
arr[2:]
# [2, 3, 4, 5]
```

#### 4️⃣ Full Copy
```python
arr[:]
# [0, 1, 2, 3, 4, 5]
```

#### 5️⃣ Step Example
```python
arr[::2]
# [0, 2, 4]
```

#### 6️⃣ Reverse List
```python
arr[::-1]
# [5, 4, 3, 2, 1, 0]
```

#### 🔴 Negative Indexing
```python
arr[-3:]
# [3, 4, 5]
```
👉 Negative index = count from end

---

## 🧠 Works on Multiple Types
- **`String`**
```python
s = "hello"
s[1:4]
# "ell"
```

- **`Tuple`**
```python
t = (1, 2, 3, 4)
t[1:3]
# (2, 3)
```

---

## ⚖️ Summary Table
| Expression  | Result              |
| ----------- | ------------------- |
| `arr[1:4]`  | elements 1 to 3     |
| `arr[:3]`   | first 3 elements    |
| `arr[2:]`   | from index 2 to end |
| `arr[::2]`  | every 2nd element   |
| `arr[::-1]` | reversed            |

---

# 🧠 Copy vs Deep Copy

| Feature | Shallow Copy (e.g. slicing [:]) | Deep Copy |
|--------|-------------------------------|----------|
| Definition | Copies outer object only | Copies all nested objects |
| Syntax | new_list = old_list[:] | import copy → copy.deepcopy(obj) |
| Nested objects | Shared ❌ | Independent ✅ |
| Memory | Less memory | More memory |
| Performance | Faster | Slower |
| Use case | Simple lists | Complex/nested structures |

## 🎯 Key Insight
| Concept      | Explanation                         |
| ------------ | ----------------------------------- |
| Slicing [:]  | Creates a shallow copy              |
| Shallow copy | Copies references of nested objects |
| Deep copy    | Fully independent copy              |

---

# 🧠 What is a Decorator in Python?
**`Decorator`** &rarr; a function that **modifies or extends the behavior of another function without changing its code**

## 🔧 Simple Explanation
👉 Instead of changing a function:
```python
def greet():
    print("Hello")
```

👉 You wrap it with extra behavior:
```python
def my_decorator(func):
    def wrapper():
        print("Before")
        func()
        print("After")
    return wrapper
```

### ✨ Using the Decorator
```python
@my_decorator
def greet():
    print("Hello")
```

#### 🎯 Output
```
Before
Hello
After
```

### 🧠 What Happens Behind the Scenes
```python
greet = my_decorator(greet)
```
👉 The function is replaced by the wrapped version

### 🧩 Key Concepts
| Concept            | Explanation                          |
| ------------------ | ------------------------------------ |
| Function as object | Functions can be passed as arguments |
| Wrapper function   | Adds extra behavior                  |
| @ syntax           | Cleaner way to apply decorator       |

---

### 🔧 Real Use Cases
#### ✅ Logging
```python
def log(func):
    def wrapper():
        print("Function called")
        return func()
    return wrapper
```

#### ✅ Authentication
- check user before running function

#### ✅ Timing
- measure execution time

#### ⚠️ Decorator with Arguments (IMPORTANT)
```python
def decorator(func):
    def wrapper(*args, **kwargs):
        print("Before")
        return func(*args, **kwargs)
    return wrapper
```    
👉 Allows flexible functions

---

### 🧠 Built-in Decorators
#### 🔹 `@staticmethod`
- no access to class or instance
#### 🔹 `@classmethod`
- works with class
#### 🔹 `@property`
- makes method behave like attributej

---

### ⚖️ Without vs With Decorator
| Without         | With         |
| --------------- | ------------ |
| Manual wrapping | Clean syntax |
| Hard to reuse   | Reusable     |

---

# 🧠 What is a Lambda Function?
**`Lambda Function`** &rarr; a **small**, anonymous (**unnamed**) **function defined in a single line**

## 🔧 Syntax
```python
lambda arguments: expression
```

## 🎯 Key Idea
- no name
- one line only
- returns a value automatically

## 🟢 Basic Example
```python
add = lambda x, y: x + y
print(add(2, 3))
# 5
```

🔄 Equivalent Normal Function
```python
def add(x, y):
    return x + y
```

## ⚖️ Lambda vs Normal Function
| Feature  | Lambda       | Normal Function |
| -------- | ------------ | --------------- |
| Name     | Anonymous    | Named           |
| Size     | One line     | Multiple lines  |
| Return   | Automatic    | Uses `return`   |
| Use case | Simple logic | Complex logic   |

---

## 🧠 Common Use Cases
### 1️⃣ With `map()`
```python
nums = [1, 2, 3]
result = list(map(lambda x: x * 2, nums))
# [2, 4, 6]
```

### 2️⃣ With `filter()`
```python
nums = [1, 2, 3, 4]
result = list(filter(lambda x: x % 2 == 0, nums))
# [2, 4]
```

### 3️⃣ With `sorted()`
```python
data = [(1, 3), (2, 1), (4, 2)]
sorted_data = sorted(data, key=lambda x: x[1])
```
👉 Sorts by second value

---

# 🧠 What is Serialization?
**`Serialization`** &rarr; the process of converting an object into a format that can be stored or transmitted

## 🎯 Why?
- save data to file
- send data over network
- share between systems

## 📦 Example
```python
data = {"name": "Alice", "age": 25}
# {"name": "Alice", "age": 25}
```

# 🧠 What is Deserialization?
**`Deserialization`** &rarr; the process of converting serialized data back into an object

# ⚖️ Serialization vs Deserialization
| Concept         | Meaning                  |
| --------------- | ------------------------ |
| Serialization   | Object &rarr; storable format |
| Deserialization | Format &rarr; object          |

---

# 🧠 What is Pickling (Python-specific)
**`Pickling`** &rarr; Python’s way of serializing objects into a binary format

## 🔧 Example
```python
import pickle

data = {"name": "Alice"}

with open("data.pkl", "wb") as f:
    pickle.dump(data, f)
```

### 🎯 Result
- object &rarr; binary file (.pkl)

# 🧠 What is Unpickling
**`Unpickling`** &rarr; is converting binary data back into a Python object

## 🔧 Example
```python
with open("data.pkl", "rb") as f:
    data = pickle.load(f)
```

# ⚖️ Pickling vs JSON
| Feature         | Pickle | JSON  |
| --------------- | ------ | ----- |
| Format          | Binary | Text  |
| Python-specific | Yes    | No    |
| Readable        | No     | Yes   |
| Security        | Risky  | Safer |

## 🧠 Big Picture Connection
```
Object → Serialization → Storage/Transfer → Deserialization → Object
```

👉 In Python:
```
Object → Pickling → File → Unpickling → Object
```

---

# 🧠 What are Generators in Python?
**`Generators`** &rarr; are functions that return values one at a time using yield, instead of returning all values at once

## 🔧 Simple Explanation
👉 Normal function:
```python
def get_numbers():
    return [1, 2, 3]
```

👉 Generator:
```python
def get_numbers():
    yield 1
    yield 2
    yield 3
```

## 🎯 Key Idea
- Lazy evaluation &rarr; values are generated only when needed
- Memory efficient &rarr; does NOT store all data at once

## 🔄 How It Works
```python
gen = get_numbers()

# for num in get_numbers():
#     print(num)

print(next(gen))  # 1
print(next(gen))  # 2
print(next(gen))  # 3
```
👉 Generates values one by one

## ⚖️ Generator vs List
| Feature   | Generator             | List                  |
| --------- | --------------------- | --------------------- |
| Memory    | Low                   | High                  |
| Execution | Lazy                  | Immediate             |
| Storage   | Not stored            | Stored in memory      |
| Speed     | Faster for large data | Slower for large data |

---

# 🧠 What is a Ternary Operator in Python?

**`Ternary Operator`** &rarr; a one-line shortcut for an if-else statement

## 🔧 Syntax
```python
value_if_true if condition else value_if_false
```

---

# 🧠 What is `*args` and `**kwargs`?
They allow a function to accept a variable number of arguments

## 1️⃣ *args (Non-keyword arguments)
`*args` &rarr; collects **positional unnamed arguments into a tuple**

### 🔧 Example
```python
def my_func(*args):
    print(args)

my_func(1, 2, 3)
# (1, 2, 3)
```

### 🎯 Key Points
- stored as a tuple
- no limit on number of arguments

### 📦 Example Usage
```python
def sum_all(*args):
    return sum(args)

print(sum_all(1, 2, 3, 4))  # 10
```

## 2️⃣ **kwargs (Keyword arguments)
`**kwargs` &rarr; collects **named arguments into a dictionary**

### 🔧 Example
```python
def my_func(**kwargs):
    print(kwargs)

my_func(name="Alice", age=25)
#{'name': 'Alice', 'age': 25}
```

### 🎯 Key Points
- stored as a dictionary
- keys = argument names

### 📦 Example Usage
```python
def print_info(**kwargs):
    for key, value in kwargs.items():
        print(key, value)
```

## ⚖️ *args vs **kwargs
| Feature | *args                | **kwargs        |
| ------- | -------------------- | --------------- |
| Type    | Tuple                | Dictionary      |
| Input   | Positional arguments | Named arguments |
| Syntax  | `*args`              | `**kwargs`      |

### 🧠 Using Both Together
```python
def func(*args, **kwargs):
    print("args:", args)
    print("kwargs:", kwargs)

func(1, 2, name="Alice", age=25)

# 👉 Output:
# args: (1, 2)
# kwargs: {'name': 'Alice', 'age': 25}
```

### ⚠️ Order Rule (IMPORTANT)
```python
def func(a, *args, **kwargs):
    pass
```

👉 Order must be:
```python
normal args → *args → **kwargs
```

---

# 🐍 Python: break vs continue vs pass

| Keyword   | Purpose | What it does | Example Behavior | Use Case |
|----------|--------|-------------|------------------|----------|
| break     | Exit loop | Stops the loop completely | Loop ends immediately | Stop when condition is met |
| continue  | Skip iteration | Skips current iteration, continues next | Jumps to next loop cycle | Skip unwanted values |
| pass      | Do nothing | Placeholder, no action | Code runs but nothing happens | Empty blocks / future code |

---

## 🔧 Examples

### 🔴 break
```python
for i in range(5):
    if i == 3:
        break
    print(i)
# 👉 Output: 0 1 2
```

### 🟡 continue
```python
for i in range(5):
    if i == 3:
        continue
    print(i)
# 👉 Output: 0 1 2 4
```

### ⚪ pass
```python
for i in range(5):
    if i == 3:
        pass
    print(i)
# 👉 Output: 0 1 2 3 4
```

## 🎯 Key Differences
| Keyword  | Loop Stops? | Skips Iteration? | Does Nothing? |
| -------- | ----------- | ---------------- | ------------- |
| break    | ✅ Yes       | ❌ No             | ❌ No          |
| continue | ❌ No        | ✅ Yes            | ❌ No          |
| pass     | ❌ No        | ❌ No             | ✅ Yes         |

---

# 🧠 What is Multithreading in Python?
**`Multithreading`** &rarr; the ability to **run multiple threads (smaller units of a process) concurrently within a program**

## 🔧 Simple Explanation
👉 Instead of doing one task at a time:
```
Task A → Task B → Task C ❌
```

👉 Multithreading:
```
Task A
Task B   → running "at the same time" ✅
Task C
```

---

# 🧩 What is a Thread?
**`Thread`** &rarr; a **lightweight unit of execution inside a process**

## 🔧 Basic Example
```python
import threading

def task():
    print("Running task")

t1 = threading.Thread(target=task)
t2 = threading.Thread(target=task)

t1.start()
t2.start()

t1.join()
t2.join()
```

## 🎯 What Happens
- Two threads start
- Tasks run concurrently
- Program waits until both finish

---

# 🧠 When to Use Multithreading
## ✅ Good for (I/O-bound tasks)
- API calls
- file reading
- network requests

## ❌ Not good for (CPU-bound tasks)
- heavy computations
- data processing

👉 Because of something very important 👇

## ⚠️ The GIL (VERY IMPORTANT)
**`GIL (Global Interpreter Lock)`** &rarr; allows only one thread to execute Python bytecode at a time

### 🎯 Impact
- Threads don’t run truly in parallel for CPU work
- Only one thread runs at a time

## ⚖️ Multithreading vs Multiprocessing
| Feature           | Multithreading | Multiprocessing    |
| ----------------- | -------------- | ------------------ |
| Threads           | Same process   | Multiple processes |
| Memory            | Shared         | Separate           |
| Speed (CPU tasks) | Limited (GIL)  | Faster             |
| Use case          | I/O tasks      | CPU tasks          |

## 🎯 One-Line Summary
Multithreading = concurrent tasks, best for I/O, limited by GIL

---

# 🧠 What is Multiprocessing in Python?
**`Multiprocessing`** &rarr; the ability to **run multiple processes in parallel, each with its own Python interpreter and memory space**

## 🔧 Simple Explanation
👉 Instead of one process with multiple threads:
```
Process → Thread A, Thread B (limited by GIL)
```

👉 Multiprocessing:
```
Process A
Process B   → truly running in parallel ✅
Process C
```

---

# 🧩 What is a Process?
**`Process`** &rarr; an **independent program execution with its own memory**

## 🔧 Basic Examplemn
```python
from multiprocessing import Process

def task():
    print("Running task")

p1 = Process(target=task)
p2 = Process(target=task)

p1.start()
p2.start()

p1.join()
p2.join()
```

## 🎯 What Happens
- Two separate processes start
- They run in parallel (true parallelism)
- Each has its own memory

---

# 🧠 Why Use Multiprocessing?
## ✅ Best for CPU-bound tasks
- heavy computations
- data processing
- machine learning

## ❌ Not ideal for
- simple I/O tasks (overhead too high)

## ⚠️ Why It Works (vs Threads)
👉 Multiprocessing bypasses the GIL
   - Each process has its own Python interpreter

---

# ⚖️ Multiprocessing vs Multithreading
| Feature           | Multithreading  | Multiprocessing |
| ----------------- | --------------- | --------------- |
| Parallelism       | ❌ Limited (GIL) | ✅ True parallel |
| Memory            | Shared          | Separate        |
| Speed (CPU tasks) | Slow            | Fast            |
| Overhead          | Low             | Higher          |
| Use case          | I/O tasks       | CPU tasks       |

## 🎯 One-Line Summary
Multiprocessing = true parallelism for CPU-heavy tasks

---

# 🧠 What is Async in Python?
**`Async (asynchronous programming)`** &rarr; allows a program to handle multiple tasks by switching between them while waiting, without blocking execution

## 🔧 Simple Explanation
👉 Normal (synchronous):
```
Task A → wait → Task B → wait → Task C ❌
```

👉 Async:
```
Task A (waiting)
Task B runs
Task C runs  ✅
```

👉 While one task waits &rarr; others continue

---

# ⚙️ Core Keywords
## 🔹 `async`
**`async`** &rarr; Defines an asynchronous function
```pyhon
async def my_func():
    pass
```

## 🔹 `await`
**`await`** &rarr; Waits for a task to finish without blocking the program
```python
await some_task()
```

### 🔧 Example
```python
import asyncio

async def task():
    print("Start")
    await asyncio.sleep(2)
    print("End")

asyncio.run(task())
```

### 🎯 What Happens
- program starts
- waits 2 seconds (non-blocking)
- continues execution

### 🧠 Multiple Tasks Example
```python
import asyncio

async def task(name):
    print(f"Start {name}")
    await asyncio.sleep(2)
    print(f"End {name}")

async def main():
    await asyncio.gather(
        task("A"),
        task("B"),
        task("C")
    )

asyncio.run(main())
``` 

---

# ⚖️ Async vs Threading vs Multiprocessing
| Feature     | Async     | Threading | Multiprocessing |
| ----------- | --------- | --------- | --------------- |
| Parallel    | ❌ No      | ❌ Limited | ✅ Yes           |
| Concurrency | ✅ Yes     | ✅ Yes     | ✅ Yes           |
| Best for    | I/O tasks | I/O tasks | CPU tasks       |
| Memory      | Low       | Medium    | High            |

---

# 🧠 Key Idea
**`Async`** &rarr; uses a single thread but switches between tasks efficiently

## 🎯 When to Use Async
### ✅ Good for:
- API calls
- database queries
- file/network I/O

### ❌ Not for:
- heavy computations (CPU-bound)

### ⚠️ Important Concept
👉 Async is:
- non-blocking
- not the same as parallelism

# 🎯 One-Line Summary
Async = non-blocking concurrency in a single thread

---

# 🧠 Python Concurrency: Threading vs Multiprocessing vs Async

| Feature | Multithreading | Multiprocessing | Async (Asynchronous) |
|--------|---------------|----------------|----------------------|
| 🧩 Unit | Threads | Processes | Coroutines (tasks) |
| ⚙️ Execution | Concurrent (not truly parallel due to GIL) | Parallel (true parallelism) | Concurrent (single thread) |
| 🧠 GIL Impact | ❌ Affected | ✅ Not affected | ❌ Affected (but not blocking) |
| 🚀 Best For | I/O-bound tasks | CPU-bound tasks | I/O-bound tasks |
| 💾 Memory | Shared memory | Separate memory | Shared (single thread) |
| ⚡ Performance | Good for I/O | Best for CPU-heavy work | Very efficient for I/O |
| 🔄 Context Switching | OS-managed | OS-managed | Event loop (lightweight) |
| 📦 Overhead | Low | High (process creation) | Very low |
| 🔗 Communication | Easy (shared memory) | Harder (queues, pipes) | Easy (within event loop) |
| 📚 Complexity | Medium | Medium | Higher (requires async/await) |
| 🧪 Example Use | API calls, file I/O | Data processing, ML | APIs, web scraping, DB calls |

---

## 🎯 Quick Decision Guide

| Scenario | Best Choice |
|---------|------------|
| API calls / network requests | Async or Threading |
| Reading files / I/O tasks | Async or Threading |
| Heavy computation / CPU work | Multiprocessing |
| Large-scale concurrent I/O | Async |

---

## 🎯 One-Line Summary

Threading &rarr; I/O (limited by GIL)  
Multiprocessing &rarr; CPU (true parallelism)  
Async &rarr; efficient I/O (non-blocking)

---

# 🧠 What is OOP (Object-Oriented Programming)?
**`OOP`** &rarr; a programming paradigm based on **organizing code into objects that contain data (attributes) and behavior (methods)**

## 🎯 Why OOP?
- better structure
- reusable code
- easier maintenance
- scalable systems

---

# 🧩 Core Concepts of OOP
## 1️⃣ Class
**`Class`** &rarr; a blueprint/template for creating objects

### 🔧 Example
```python
class Person:
    def __init__(self, name):
        self.name = name
```

### 🎯 Think of it like:
👉 A blueprint of a house

---

## 2️⃣ Object
**`Object`** &rarr; instance of a class

### 🔧 Example
```python
p1 = Person("Alice")
```

### 🎯 Think of it like:
👉 A real house built from blueprint

---

## 3️⃣ Encapsulation (IMPORTANT)
**`Encapsulation`** &rarr; means **hiding internal data and controlling access to it**

### 🔧 Example
```python
class Person:
    def __init__(self):
        self.__age = 0  # private

    def set_age(self, age):
        self.__age = age
```

### 🎯 Key Idea
- protect data
- use getters/setters

---

## 4️⃣ Inheritance
**`Inheritance`** &rarr; allows a class to **inherit properties and methods from another class**

### 🔧 Example
```python
class Animal:
    def speak(self):
        print("Sound")

class Dog(Animal):
    pass
```
👉 Dog inherits from Animal

### 🎯 Benefit
- reuse code
- avoid duplication

---

## 5️⃣ Polymorphism
**`Polymorphism`** &rarr; **same method name behaves differently depending on the object**

### 🔧 Example
```python
class Dog:
    def speak(self):
        print("Bark")

class Cat:
    def speak(self):
        print("Meow")
        
for animal in [Dog(), Cat()]:
    animal.speak()
```
👉 Different behavior, same method name

---

## 6️⃣ Abstraction
**`Abstraction`** &rarr; hiding complex logic and showing only essential features

---

## ⚖️ Summary Table
| Concept       | Meaning                         |
| ------------- | ------------------------------- |
| Class         | Blueprint                       |
| Object        | Instance of class               |
| Encapsulation | Hide data                       |
| Inheritance   | Reuse code                      |
| Polymorphism  | Same method, different behavior |
| Abstraction   | Hide complexity                 |

---

# 🧠 Additional Important Concepts
## 🔹 Constructor (`__init__`)
- initializes object
## 🔹 self
- refers to current object
## 🔹 Method
- function inside class
## 🔹 Attribute
- variable inside class

---

# 🧠 What does `_` mean in Python?
**`_`** &rarr; is a convention-based symbol in Python used in multiple ways (not a single fixed meaning)

## 📊 Different Uses of `_`
| Usage | Meaning | Example | Explanation |
|------|--------|--------|------------|
| 1️⃣ Throwaway variable | Ignore value | for _ in range(5): | Value not needed |
| 2️⃣ Last result (REPL) | Stores last output | _ + 2 | Used in interactive shell |
| 3️⃣ Private variable (convention) | Internal use | _name | Should not be accessed outside |
| 4️⃣ Strong private (name mangling) | Avoid override | __name | Becomes _ClassName__name |
| 5️⃣ Special methods | Built-in behavior | __init__ | Python internal methods |

---

# 🧠 What is an Empty Class in Python?
**`Empty Class`** &rarr; a class that has no attributes or methods defined inside it

## 🔧 Example
```python
class MyClass:
    pass
```

## 🎯 Key Idea
- pass is required because Python expects a block
- the class does nothing (for now)
- it’s a placeholder

---

# 🧩 Why Use an Empty Class?
## ✅ 1. Placeholder for future code
```python
class User:
    pass
``` 
👉 You plan to add logic later

## ✅ 2. Simple object container
```python
class Data:
    pass

d = Data()
d.name = "Alice"
```
👉 You can dynamically add attributes

## ✅ 3. Marker / Tag class
Used to identify types:
```python
class Event:
    pass
```

---

# ⚖️ Empty Class vs Full Class
| Feature    | Empty Class      | Full Class |
| ---------- | ---------------- | ---------- |
| Attributes | None (initially) | Defined    |
| Methods    | None             | Defined    |
| Use case   | Placeholder      | Real logic |

## 🧠 Related Concept: pass
pass is used to define an empty block in Python

---

# 🧠 What is Monkey Patching?
**`Monkey Patching`** &rarr; the **practice of dynamically modifying or extending code (classes, functions, or modules) at runtime**

## 🔧 Simple Explanation
👉 You change behavior without modifying the original source code

## 🟢 Example
```python
class Dog:
    def speak(self):
        return "Bark"
```

### 🐒 Monkey Patch it:
```python
def new_speak(self):
    return "Meow"

Dog.speak = new_speak
```

### 🎯 Result
```python
d = Dog()
print(d.speak())
# 👉 Output:
# Meow
```

---

## 🎯 Use Cases
### ✅ 1. Testing / Mocking
- replace functions during tests
### ✅ 2. Fixing bugs temporarily
- patch library behavior
### ✅ 3. Extending libraries
- add features without modifying source

### ⚠️ Risks (VERY IMPORTANT)
❌ Problems
- hard to debug
- unexpected behavior
- breaks code consistency
- affects entire program

## 🎯 One-Line Summary
Monkey patching = change code behavior at runtime

---