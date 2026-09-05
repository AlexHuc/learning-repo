# Linux Fundamentals

Linux is a popular choice for servers and the machines that you'll interact within cyber security.

## Linux Commands & File Permissions Cheat Sheet

### 1. Basic Linux Commands

| Command  | Purpose                                                      | Example                            |
| -------- | ------------------------------------------------------------ | ---------------------------------- |
| `whoami` | Tells you which user you are logged in as                    | `whoami`                           |
| `echo`   | Outputs the text you provide                                 | `echo "Hello"`                     |
| `ls`     | Lists the contents of the current directory                  | `ls`                               |
| `cd`     | Changes directory — moves into a folder                      | `cd /home/user`                    |
| `cat`    | Displays the contents of a file                              | `cat file.txt`                     |
| `pwd`    | Prints the current working directory — "where am I?"         | `pwd`                              |
| `find`   | Searches for **files and directories**, commonly **by name** | `find -name passwords.txt`         |
| `grep`   | Searches **inside files for matching text**                  | `grep "password123" passwords.txt` |

> **Remember:**  
> `find` searches for **files/directories**, while `grep` searches for **text inside files**.

---

### 2. Command Operators & Redirection

| Operator | Purpose                                                                                                | Example                    |
| -------- | ------------------------------------------------------------------------------------------------------ | -------------------------- |
| `&`      | Runs a command in the background instead of waiting for it to finish. Useful for long-running commands | `sleep 60 &`               |
| `&&`     | Runs the second command only after the first command finishes successfully                             | `mkdir test && cd test`    |
| `>`      | Redirects command output to a file. **Overwrites** existing content                                    | `echo "Hello" > file.txt`  |
| `>>`     | Redirects command output to a file. **Appends** to existing content                                    | `echo "World" >> file.txt` |

#### `&` — Background Execution

```bash
command &
```

The command runs in the background, allowing you to continue using the terminal.

#### `&&` — Sequential Execution

```bash
command1 && command2
```

`command2` runs after `command1` completes successfully.

#### `>` — Overwrite

```bash
echo "Hello" > file.txt
```

Creates `file.txt` or replaces its existing contents.

#### `>>` — Append

```bash
echo "Hello" >> file.txt
```

Creates `file.txt` if needed, or adds the output to the end of the existing file.

---

### 3. SSH — Secure Shell

**SSH (Secure Shell)** is a protocol used to securely communicate with and access another computer over a network.

SSH encrypts the communication between the client and remote machine. This means information sent across the network, such as commands and other input, is protected from being transmitted as readable plain text.

A typical SSH connection looks like:

```bash
Your Computer
     |
     |  Encrypted SSH connection
     v
Remote Linux Machine
```

---

### 4. The `man` — Manual Pages

Linux provides built-in documentation for many commands through **manual pages**, commonly accessed with the `man` command.

#### Syntax

```bash
man <command>
```

#### Example

```bash
man ls
```

This displays the manual page for `ls`, including its available options and usage.

You can also find Linux manual pages online:

- https://linux.die.net/man/

> **Tip:** When you encounter an unfamiliar command or option, try `man <command>` first.

---

### 5. Basic File & Directory Management

| Command | Full Name / Meaning | Purpose                                             | Example                   |
| ------- | ------------------- | --------------------------------------------------- | ------------------------- |
| `touch` | `touch`             | Creates an empty file or updates a file's timestamp | `touch file.txt`          |
| `mkdir` | Make Directory      | Creates a directory                                 | `mkdir myfolder`          |
| `cp`    | Copy                | Copies a file or directory                          | `cp file.txt backup.txt`  |
| `mv`    | Move                | Moves or renames a file or directory                | `mv file.txt newfile.txt` |
| `rm`    | Remove              | Removes a file or directory                         | `rm file.txt`             |
| `file`  | File                | Determines the type of a file                       | `file image.jpg`          |

#### Common Examples

```bash
# Create a file
touch notes.txt

# Create a directory
mkdir projects

# Copy a file
cp notes.txt notes-backup.txt

# Rename a file
mv notes.txt old-notes.txt

# Remove a file
rm old-notes.txt

# Determine file type
file image.jpg
```

> **Caution:** `rm` normally removes files without sending them to a recycle bin.

---

### 6. Linux File Permissions

Linux uses permissions to control who can **read, write, or execute** files and directories.

A typical permission string looks like:

```bash
rwxrwxrwx
```

The permissions are divided into three groups:

| Section            | Applies To | Example |
| ------------------ | ---------- | ------- |
| First 3 characters | Owner      | `rwx`   |
| Next 3 characters  | Group      | `rwx`   |
| Last 3 characters  | Others     | `rwx`   |

#### Permission Types

| Symbol | Permission    | Meaning                               |
| ------ | ------------- | ------------------------------------- |
| `r`    | Read          | Read the contents of a file.          |
| `w`    | Write         | Modify the contents of a file.        |
| `x`    | Execute       | Execute a file or access a directory. |
| `-`    | No permission | The permission is not granted.        |

---

### 7. Reading `ls -l` Permissions

Running:

```bash
ls -lh
```

can produce output such as:

```bash
-rw-r--r-- 1 cmnatic cmnatic 0 Feb 19 10:37 file1
-rw-r--r-- 8 cmnatic cmnatic 0 Feb 19 10:37 file2
```

The permission section is the first part:

```bash
-rw-r--r--
```

Break it down:

```bash
- rw- r-- r--
  |   |   |
  |   |   +--- Others
  |   +------- Group
  +----------- Owner
```

The first character indicates the file type:

| Character | Meaning       |
| --------- | ------------- |
| `-`       | Regular file  |
| `d`       | Directory     |
| `l`       | Symbolic link |

---

### 8. Numeric Linux Permissions

Linux permissions can also be represented using numbers.

| Permission          | Numeric Value |
| ------------------- | ------------: |
| Read (`r`)          |             4 |
| Write (`w`)         |             2 |
| Execute (`x`)       |             1 |
| No permission (`-`) |             0 |

To calculate a permission value, add the permissions together.

#### Example

```bash
rwx = 4 + 2 + 1 = 7
rw- = 4 + 2 + 0 = 6
r-x = 4 + 0 + 1 = 5
r-- = 4 + 0 + 0 = 4
```

---

### 9. Symbolic → Numeric Permission Conversion

For:

```bash
rwxrwxrwx
```

split the permissions into three groups:

| Group  | Permissions | Calculation | Value |
| ------ | ----------- | ----------- | ----: |
| Owner  | `rwx`       | `4 + 2 + 1` |     7 |
| Group  | `rwx`       | `4 + 2 + 1` |     7 |
| Others | `rwx`       | `4 + 2 + 1` |     7 |

Therefore:

```bash
rwxrwxrwx = 777
```

---

### 10. Common Permission Values

| Symbolic    | Numeric | Meaning                                                              |
| ----------- | ------: | -------------------------------------------------------------------- |
| `rwxrwxrwx` |   `777` | Everyone can read, write, and execute                                |
| `rwxr-xr-x` |   `755` | Owner has full access; group and others can read and execute         |
| `rw-r--r--` |   `644` | Owner can read/write; group and others can only read                 |
| `rwx------` |   `700` | Only the owner has full access                                       |
| `rwxr-x---` |   `750` | Owner has full access; group can read/execute; others have no access |
| `rw-------` |   `600` | Owner can read/write; nobody else has access                         |

---

### 11. `chmod` — Changing Permissions

`chmod` changes the permissions of a file or directory.

#### Syntax

```bash
chmod <permissions> <file>
```

#### Example

```bash
chmod 750 system_overview.txt
```

This gives:

| User   | Permissions            | Numeric |
| ------ | ---------------------- | ------: |
| Owner  | Read + Write + Execute |       7 |
| Group  | Read + Execute         |       5 |
| Others | No permissions         |       0 |

Therefore:

```bash
750 = rwxr-x---
```

> **Why permissions matter:**  
> Correct permissions help prevent unauthorized users from reading, modifying, or executing files.

---

### 12. Users & Groups

Linux permissions are based around three categories:

```bash
Owner
Group
Others
```

A user can own a file, while a group of users can have a separate set of permissions for that same file.

For example:

```bash
Owner  → read + write
Group  → read
Others → no access
```

This allows Linux systems to provide granular access control without requiring every user to have the same permissions.

#### Real-World Example

A web server may need permission to read and write certain application files.

At the same time, individual customers may need permission to upload their own website files without becoming the web server's system user.

Using separate users, groups, and permissions helps isolate access and improve security.

---

### 13. Switching Users with `su`

The `su` command allows you to switch to another user account.

#### Basic Syntax

```bash
su <username>
```

Example:

```bash
su user2
```

You will normally need the password of the target user unless you already have sufficient privileges.

Example:

```bash
tryhackme@linux2:~$ su user2
Password:
user2@linux2:/home/tryhackme$
```

---

### 14. `su -l` / `su --login`

The `-l` option starts a login shell for the target user.

```bash
su -l user2
```

or:

```bash
su --login user2
```

A login shell loads more of the target user's normal environment and typically places you in that user's home directory.

Example:

```bash
tryhackme@linux2:~$ su -l user2
Password:
user2@linux2:~$ pwd
/home/user2
```

#### Difference

| Command       | Behavior                                                               |
| ------------- | ---------------------------------------------------------------------- |
| `su user2`    | Switches to `user2` but largely retains the current environment.       |
| `su -l user2` | Starts a login shell as `user2`, loading the user's login environment. |

---

### 15. Important Linux Directories

Linux has a standard filesystem hierarchy. Some directories are particularly important when navigating or administering a system.

| Directory | Purpose                                                 |
| --------- | ------------------------------------------------------- |
| `/`       | Root of the entire filesystem                           |
| `/home`   | Home directories for regular users                      |
| `/etc`    | System-wide configuration files                         |
| `/var`    | Variable data such as logs and application/service data |
| `/root`   | Home directory of the root user                         |
| `/tmp`    | Temporary files and data                                |

---

### 16. `/etc`

The `/etc` directory is a major location for **system-wide configuration files**.

Examples include:

```bash
/etc/passwd
/etc/shadow
/etc/sudoers
/etc/sudoers.d/
```

#### Notable Files

| Path              | Purpose                                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------------------- |
| `/etc/passwd`     | Contains information about local user accounts                                                                    |
| `/etc/shadow`     | Stores password-related authentication data and password hashes on systems using traditional local authentication |
| `/etc/sudoers`    | Defines rules for which users/groups can use `sudo` and what they may run                                         |
| `/etc/sudoers.d/` | Additional `sudo` configuration files                                                                             |

Example:

```bash
cd /etc
ls
```

---

### 17. `/var`

`/var` stands for **variable data**.

It contains data that is frequently changed by the operating system, services, or applications.

A particularly important directory is:

```bash
/var/log
```

which commonly contains system and application logs.

Example:

```bash
cd /var
ls
```

Possible contents include:

```bash
backups
log
opt
tmp
```

---

### 18. `/root`

`/root` is the **home directory of the root user**.

It is different from:

```bash
/home
```

Regular users commonly have home directories such as:

```bash
/home/alice
/home/bob
```

while the root user's home directory is:

```bash
/root
```

Example:

```bash
cd /root
```

Access to `/root` is normally restricted to the root user or users with sufficient privileges.

---

### 19. `/tmp`

`/tmp` is used for **temporary files**.

Typical characteristics:

- Used by applications and users for temporary data.
- Often writable by multiple users, subject to the directory's permissions and system security mechanisms.
- Temporary files may be removed automatically depending on the operating system and its configuration.
- Data should generally not be assumed to persist indefinitely.

Example:

```bash
cd /tmp
ls
```

Possible contents:

```bash
todelete
trash.txt
rubbish.bin
```

> **Pentesting note:** `/tmp` is commonly useful for temporarily storing scripts or files during an authorized assessment because it is generally writable by regular users. Always ensure your activity is authorized.

---

### 20. Quick Command Reference

| Command / Operator | What It Does                                     |
| ------------------ | ------------------------------------------------ |
| `whoami`           | Show the current user                            |
| `echo`             | Print text/output                                |
| `ls`               | List directory contents                          |
| `cd`               | Change directory                                 |
| `pwd`              | Show the current directory                       |
| `cat`              | Display file contents                            |
| `find`             | Search for files/directories                     |
| `grep`             | Search inside text for matches                   |
| `touch`            | Create a file / update timestamp                 |
| `mkdir`            | Create a directory                               |
| `cp`               | Copy files/directories                           |
| `mv`               | Move or rename files/directories                 |
| `rm`               | Remove files/directories                         |
| `file`             | Identify a file's type                           |
| `man`              | Display command documentation                    |
| `su`               | Switch users                                     |
| `chmod`            | Change file permissions                          |
| `&`                | Run a command in the background.                 |
| `&&`               | Run the next command after successful completion |
| `>`                | Redirect output and overwrite a file             |
| `>>`               | Redirect output and append to a file             |

---

### 21. Permission Quick Reference

```bash
r = 4
w = 2
x = 1
```

| Permission | Value |
| ---------- | ----: |
| `---`      |     0 |
| `--x`      |     1 |
| `-w-`      |     2 |
| `-wx`      |     3 |
| `r--`      |     4 |
| `r-x`      |     5 |
| `rw-`      |     6 |
| `rwx`      |     7 |

#### Remember the Pattern

```bash
        Owner   Group   Others
          ↓       ↓       ↓
        rwx     rwx     rwx
         7       7       7
```

For example:

```bash
rwxr-x---
  7   5   0

= 750
```

The three digits always represent:

```bash
[Owner][Group][Others]
```