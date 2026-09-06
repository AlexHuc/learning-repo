# Linux Fundamentals

Linux is a popular choice for servers and the machines that you'll interact within cyber security.

## Linux Commands & File Permissions Cheat Sheet

### 1. Basic Linux Commands

| Command  | Purpose                                                      | Example                            |
| -------- | ------------------------------------------------------------ | ---------------------------------- |
| `whoami` | Tells you which user you are logged in as                    | `whoami`                           |
| `echo`   | Outputs the text you provide                                 | `echo "Hello"`                     |
| `ls`     | Lists the contents of the current directory                  | `ls`                               |
| `cd`     | Changes directory - moves into a folder                      | `cd /home/user`                    |
| `cat`    | Displays the contents of a file                              | `cat file.txt`                     |
| `pwd`    | Prints the current working directory - "where am I?"         | `pwd`                              |
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

#### `&` - Background Execution

```bash
command &
```

The command runs in the background, allowing you to continue using the terminal.

#### `&&` - Sequential Execution

```bash
command1 && command2
```

`command2` runs after `command1` completes successfully.

#### `>` - Overwrite

```bash
echo "Hello" > file.txt
```

Creates `file.txt` or replaces its existing contents.

#### `>>` - Append

```bash
echo "Hello" >> file.txt
```

Creates `file.txt` if needed, or adds the output to the end of the existing file.

---

### 3. SSH - Secure Shell

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

### 4. The `man` - Manual Pages

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

### 11. `chmod` - Changing Permissions

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

## Linux Text Editors & File Transfer Cheat Sheet

### 1. Terminal Text Editors

Terminal text editors allow you to create and modify files directly from the command line.

Two common editors are:

| Editor       | Difficulty        | Main Use                                             |
| ------------ | ----------------- | ---------------------------------------------------- |
| `nano`       | Beginner-friendly | Simple text editing                                  |
| `vim` / `vi` | Advanced          | Powerful editing, programming, system administration |

---

### 2. Nano

`nano` is a simple, beginner-friendly terminal text editor.

### Open or Create a File

```bash
nano filename
```

For example:

```bash
nano myfile
```

> If `myfile` doesn't exist, Nano will create it when you save.

#### Nano Interface

When Nano opens, you will see something similar to:

```bash
GNU nano 4.8                                             myfile

^G Get Help    ^O Write Out   ^W Where Is    ^K Cut Text
^X Exit        ^R Read File   ^\ Replace     ^U Paste Text
```

The `^` symbol represents the **Ctrl** key.

For example:

```bash
^X = Ctrl + X
```

---

### 3. Basic Nano Shortcuts

| Shortcut   | Action                              |
| ---------- | ----------------------------------- |
| `Ctrl + X` | Exit Nano                           |
| `Ctrl + O` | Write Out / Save the file           |
| `Ctrl + W` | Search for text                     |
| `Ctrl + K` | Cut the current line                |
| `Ctrl + U` | Paste previously cut text           |
| `Ctrl + J` | Justify text                        |
| `Ctrl + C` | Show the current cursor position    |
| `Ctrl + T` | Spell check                         |
| `Ctrl + _` | Go to a specific line               |
| `Alt + U`  | Undo                                |
| `Alt + E`  | Redo                                |
| `Alt + A`  | Start/stop text selection           |
| `Alt + 6`  | Copy the current line/selected text |

> **Remember:** In Nano, `^` means **Ctrl** and `M-` generally means **Alt**.

---

### 4. Saving & Exiting Nano

A common workflow is:

#### Step 1 - Open the file

```bash
nano myfile
```

### Step 2 - Write your content

```bash
Hello TryHackMe
I can write things into "myfile"
```

### Step 3 - Save

Press:

```bash
Ctrl + O
```

Nano will ask for the filename.

Press **Enter** to confirm.

### Step 4 - Exit

Press:

```bash
Ctrl + X
```

---

### 5. Useful Nano Features

Nano provides the basic functionality needed for everyday terminal editing:

| Feature     | Shortcut   |
| ----------- | ---------- |
| Search text | `Ctrl + W` |
| Save file   | `Ctrl + O` |
| Exit editor | `Ctrl + X` |
| Cut line    | `Ctrl + K` |
| Paste       | `Ctrl + U` |
| Go to line  | `Ctrl + _` |
| Undo        | `Alt + U`  |
| Redo        | `Alt + E`  |
| Copy        | `Alt + 6`  |

---

### 6. VIM

`VIM` is a powerful and highly customizable terminal text editor.

It is more complex than Nano and has a steeper learning curve, but it provides many advanced features.

#### Open a File

```bash
vim filename
```

For example:

```bash
vim myfile
```

---

### 7. Why Learn VIM?

Some advantages of VIM include:

| Feature               | Description                                                 |
| --------------------- | ----------------------------------------------------------- |
| Customizable          | Keyboard shortcuts and behavior can be customized           |
| Syntax Highlighting   | Useful when writing and maintaining source code             |
| Powerful Editing      | Provides advanced navigation and editing capabilities       |
| Terminal Availability | Vim/vi is available on many Linux and Unix-like systems     |
| Extensive Resources   | There are many tutorials, cheatsheets, and guides available |

> **Note:** You don't need to memorize every VIM feature immediately. Start with basic navigation, editing, saving, and exiting.

---

### 8. VIM Basic Modes

One of the most important concepts in VIM is that it uses different **modes**.

| Mode              | Purpose                                   |
| ----------------- | ----------------------------------------- |
| Normal mode       | Navigate and perform commands             |
| Insert mode       | Type and edit text                        |
| Command-line mode | Enter commands such as saving or quitting |

#### Enter Insert Mode

From Normal mode, press:

```bash
i
```

You can now type text.

#### Return to Normal Mode

Press:

```bash
Esc
```

#### Save

From Normal mode:

```bash
:w
```

### Quit

```bash
:q
```

### Save and Quit

```bash
:wq
```

### Quit Without Saving

```bash
:q!
```

> **Tip:** When you're unsure which mode you're in, press `Esc` to return to Normal mode.

---

### 9. Nano vs VIM

| Feature                      | Nano      | VIM  |
| ---------------------------- | --------- | ---- |
| Beginner friendly            | ✅          ❌   |
| Easy to remember             | ✅        | ❌   |
| Advanced editing             | Limited   | ✅   |
| Syntax highlighting          | Available | ✅   |
| Highly customizable          | Limited   | ✅   |
| Simple configuration editing | ✅        | ✅   |
| Steep learning curve         | Low       | High |

#### Recommendation

For beginners:

```bash
nano filename
```

is usually the easiest choice.

As you become more comfortable with Linux, learning VIM can significantly improve your command-line editing skills.

---

### 10. Downloading Files with `wget`

`wget` is a command-line utility used to download files from the web.

It commonly retrieves files over HTTP or HTTPS.

#### Basic Syntax

```bash
wget <URL>
```

#### Example

```bash
wget https://example.com/myfile.txt
```

This downloads `myfile.txt` into the current directory.

#### Check Where You Are First

```bash
pwd
```

Then download:

```bash
wget https://example.com/myfile.txt
```

The file will normally be saved in the current working directory.

---

### 11. Common `wget` Workflow

```bash
# Check current directory
pwd

# List existing files
ls

# Download a file
wget https://example.com/file.txt

# Confirm that it was downloaded
ls
```

#### Useful Options

| Option   | Purpose                                     | Example                  |
| -------- | ------------------------------------------- | ------------------------ |
| `-O`     | Save the download using a specific filename | `wget -O output.txt URL` |
| `-q`     | Quiet mode                                  | `wget -q URL`            |
| `-c`     | Continue a partially downloaded file        | `wget -c URL`            |
| `--help` | Show available options                      | `wget --help`            |

> **Tip:** Use `man wget` to see the complete documentation.

---

### 12. SCP - Secure Copy

`scp` stands for **Secure Copy**.

It allows files and directories to be transferred between computers using SSH.

Unlike the local `cp` command, which copies files on the same system, `scp` can transfer files between a local machine and a remote machine.

SCP provides:

- SSH-based authentication
- Encrypted transfer
- Local &rarr; remote transfers
- Remote &rarr; local transfers

---

### 13. SCP - Local to Remote

The general format is:

```bash
scp <SOURCE> <DESTINATION>
```

For example:

```bash
scp important.txt ubuntu@192.168.1.30:/home/ubuntu/transferred.txt
```

#### Breakdown

```bash
important.txt
```

The local file being copied.

```bash
ubuntu@192.168.1.30
```

The remote username and IP address.

```bash
/home/ubuntu/transferred.txt
```

The destination path and filename on the remote machine.

#### Direction

```bash
LOCAL MACHINE
     |
     | scp
     v
REMOTE MACHINE
```

---

### 14. SCP - Remote to Local

You can reverse the source and destination:

```bash
scp ubuntu@192.168.1.30:/home/ubuntu/documents.txt notes.txt
```

This means:

```bash
REMOTE MACHINE
     |
     | scp
     v
LOCAL MACHINE
```

The remote file:

```bash
/home/ubuntu/documents.txt
```

is downloaded and saved locally as:

```bash
notes.txt
```

---

### 15. SCP Variables

#### Local &rarr; Remote

| Variable           | Example                        |
| ------------------ | ------------------------------ |
| Remote IP          | `192.168.1.30`                 |
| Remote user        | `ubuntu`                       |
| Local file         | `important.txt`                |
| Remote destination | `/home/ubuntu/transferred.txt` |

Command:

```bash
scp important.txt ubuntu@192.168.1.30:/home/ubuntu/transferred.txt
```

#### Remote &rarr; Local

| Variable       | Example                      |
| -------------- | ---------------------------- |
| Remote IP      | `192.168.1.30`               |
| Remote user    | `ubuntu`                     |
| Remote file    | `/home/ubuntu/documents.txt` |
| Local filename | `notes.txt`                  |

Command:

```bash
scp ubuntu@192.168.1.30:/home/ubuntu/documents.txt notes.txt
```

---

### 16. SCP - Copying Directories

To recursively copy a directory, use `-r`:

```bash
scp -r myfolder ubuntu@192.168.1.30:/home/ubuntu/
```

The `-r` option tells SCP to copy the directory and its contents recursively.

> **Tip:** Always verify the destination path and permissions before transferring important files.

---

### 17. Python HTTP Server

Python 3 includes a lightweight HTTP server that can be used to serve files from a directory.

This is useful when you want another computer to download files from your machine using HTTP.

#### Start the Server

First, move into the directory containing the files:

```bash
cd /webserver
```

Then run:

```bash
python3 -m http.server
```

By default, the server listens on port `8000`.

Example:

```bash
Serving HTTP on 0.0.0.0 port 8000 (http://0.0.0.0:8000/) ...
```

---

### 18. How Python HTTP Server Works

The server exposes files from the directory where you started it.

For example:

```bash
/webserver
├── file
├── document.txt
└── script.sh
```

Run:

```bash
cd /webserver
python3 -m http.server
```

The files can then be requested through:

```bash
http://<server-ip>:8000/<filename>
```

For example:

```bash
http://192.168.1.30:8000/file
```

---

### 19. Downloading from the Python Server

Keep the Python server running in one terminal.

Open a **second terminal** and use `wget`.

For example:

```bash
wget http://192.168.1.30:8000/file
```

The workflow looks like:

```bash
Terminal 1
    |
    | python3 -m http.server
    |
    v
HTTP Server
    ^
    |
    | HTTP download
    |
Terminal 2
    |
    | wget http://192.168.1.30:8000/file
```

> **Important:** The Python HTTP server occupies the terminal while it is running. Open another terminal for commands such as `wget`.

---

# 20. Changing the HTTP Server Port

The default port is `8000`.

You can specify another port:

```bash
python3 -m http.server 9000
```

The server will then listen on port `9000`.

A client would download a file using:

```bash
wget http://192.168.1.30:9000/file
```

---

### 21. Python HTTP Server - Useful Options

You can view the available options with:

```bash
python3 -m http.server --help
```

Commonly useful options include:

| Option               | Purpose                                         |
| -------------------- | ----------------------------------------------- |
| No option            | Serve the current directory on port `8000`      |
| `<port>`             | Use a specific port                             |
| `--directory <path>` | Serve files from a specific directory           |
| `--bind <address>`   | Bind the server to a specific address/interface |

Example:

```bash
python3 -m http.server 8000 --directory /tmp
```

---

### 22. `wget` + Python HTTP Server

#### Server

On the machine containing the file:

```bash
cd /webserver
python3 -m http.server 8000
```

#### Client

On another machine:

```bash
wget http://192.168.1.30:8000/file
```

#### Result

The client downloads:

```bash
file
```

from the server's `/webserver` directory.

---

### 23. File Transfer Methods Compared

| Method             | Direction           | Protocol / Mechanism | Main Use                                |
| ------------------ | ------------------- | -------------------- | --------------------------------------- |
| `cp`               | Local &rarr; Local  | Filesystem           | Copy files on the same machine          |
| `mv`               | Local &rarr; Local  | Filesystem           | Move or rename files                    |
| `wget`             | Remote &rarr; Local | HTTP/HTTPS           | Download files from a web server        |
| `scp`              | Local ↔ Remote      | SSH                  | Securely transfer files between systems |
| Python HTTP Server | Local &rarr; Remote | HTTP                 | Quickly serve files for download        |

---

### 24. Quick Reference

#### Text Editing

| Command / Shortcut | Purpose                      |
| ------------------ | ---------------------------- |
| `nano file`        | Open/create a file with Nano |
| `Ctrl + O`         | Save in Nano                 |
| `Ctrl + X`         | Exit Nano                    |
| `Ctrl + W`         | Search in Nano.              |
| `vim file`         | Open/create a file with VIM  |
| `i`                | Enter Insert mode in VIM     |
| `Esc`              | Return to Normal mode in VIM |
| `:w`               | Save in VIM                  |
| `:q`               | Quit VIM                     |
| `:wq`              | Save and quit VIM            |
| `:q!`              | Quit without saving          |

#### File Transfer

| Command                             | Purpose                                     |
| ----------------------------------- | ------------------------------------------- |
| `wget URL`                          | Download a file over HTTP/HTTPS             |
| `scp file user@host:/path/`         | Copy a local file to a remote system        |
| `scp user@host:/path/file .`        | Copy a remote file to the current directory |
| `scp -r directory user@host:/path/` | Recursively copy a directory                |
| `python3 -m http.server`            | Start an HTTP server on port `8000`         |
| `python3 -m http.server 9000`       | Start an HTTP server on port `9000`         |

---

### 25. Common Workflows

#### Edit a File with Nano

```bash
nano notes.txt
```

Then:

```bash
Ctrl + O  → Save
Ctrl + X  → Exit
```

---

#### Download a File

```bash
wget https://example.com/file.txt
```

Then verify:

```bash
ls
```

---

#### Transfer a File to a Remote Machine

```bash
scp file.txt user@192.168.1.30:/home/user/
```

---

#### Download a File from a Remote Machine

```bash
scp user@192.168.1.30:/home/user/file.txt .
```

The `.` means the **current directory**.

---

#### Serve Files with Python

On the machine containing the files:

```bash
cd /webserver
python3 -m http.server 8000
```

On the receiving machine:

```bash
wget http://192.168.1.30:8000/file
```

---

### 26. Key Concepts to Remember

#### Nano

Simple terminal editor:

```bash
nano filename
```

Best starting point for beginners.

#### VIM

Powerful terminal editor with modes:

```bash
Normal → Insert → Normal
```

Save and quit:

```bash
:wq
```

#### Wget

Downloads files over HTTP/HTTPS:

```bash
wget URL
```

#### SCP

Securely transfers files using SSH:

```bash
scp SOURCE DESTINATION
```

#### Python HTTP Server

Quickly serves files from a directory:

```bash
python3 -m http.server 8000
```

### The Big Picture

```bash
EDIT
  |
  +--> nano
  |
  +--> vim

TRANSFER
  |
  +--> wget       → Download over HTTP/HTTPS
  |
  +--> scp        → Transfer over SSH
  |
  +--> HTTPServer → Serve files over HTTP
```