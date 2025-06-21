package main

import (
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alitto/pond"
)

// FileInfo represents enhanced file information
type FileInfo struct {
	Name       string
	Mode       fs.FileMode
	Size       int64
	ModTime    time.Time
	AccessTime time.Time
	ChangeTime time.Time
	Inode      uint64
	Blocks     int64
	Links      uint64
	Uid        uint32
	Gid        uint32
	Major      uint32
	Minor      uint32
	IsDir      bool
	IsSymlink  bool
	LinkTarget string
	Flags      uint32
}

// Options represents command line options
type Options struct {
	One           bool // -1
	All           bool // -a
	AlmostAll     bool // -A
	Classify      bool // -F
	NoSort        bool // -f
	LongFormat    bool // -l
	GroupFormat   bool // -g
	NumericFormat bool // -n
	Columns       bool // -C
	Stream        bool // -m
	Comma         bool // -x
	Directory     bool // -d
	Human         bool // -h
	Inode         bool // -i
	Kilobytes     bool // -k
	Follow        bool // -L
	NoFollow      bool // -H
	Flags         bool // -o
	Slash         bool // -p
	Quote         bool // -q
	Recursive     bool // -R
	Reverse       bool // -r
	SizeSort      bool // -S
	Blocks        bool // -s
	TimeSort      bool // -t
	AccessTime    bool // -u
	ChangeTime    bool // -c
	FullTime      bool // -T
}

var opts Options
var pool *pond.WorkerPool

const (
	BLOCKSIZE   = 512
	MAX_WORKERS = 64
)

func main() {
	args := os.Args[1:]

	// Check for --help flag
	for _, arg := range args {
		if arg == "--help" {
			printHelp()
			return
		}
	}

	// Initialize worker pool
	maxWorkers := min(MAX_WORKERS, runtime.NumCPU()*4)
	pool = pond.New(maxWorkers, maxWorkers*2)
	defer pool.StopAndWait()

	files := parseArgs(args)

	if len(files) == 0 {
		files = []string{"."}
	}

	// Process files concurrently
	processFiles(files)
}

func printHelp() {
	fmt.Println(`NAME
     ls -- list directory contents

SYNOPSIS
     ls [-1AaCcdFfgHhikLlmnopqRrSsTtux] [file ...]

DESCRIPTION
     The ls utility lists information about files and directories. By default, it lists one entry per line to standard output.

OPTIONS
     The following options are available:

     -1      (The numeric digit "one".) Force output to be one entry per line.
     -A      List all entries except for '.' and '..'. Always set for the superuser.
     -a      Include directory entries whose names begin with a dot ('.').
     -C      Force multi-column output; this is the default when output is to a terminal.
     -c      Use time file's status was last changed instead of last modification time.
     -d      Directories are listed as plain files (not searched recursively).
     -F      Display indicators after certain file types (*/=>@|).
     -f      Output is not sorted. This option implies -a.
     -g      List in long format as in -l, except that the owner is not printed.
     -H      Follow symbolic links specified on the command line.
     -h      When used with long format, use human-readable sizes.
     -i      For each file, print its inode number.
     -k      Modifies the -s option, causing sizes to be reported in kilobytes.
     -L      Follow symbolic links to show information about the linked-to file.
     -l      (The lowercase letter "ell".) List in long format.
     -m      Stream output format; list files across the page, separated by commas.
     -n      List in long format with numeric user and group IDs.
     -o      Include file flags in long format output.
     -p      Display a slash ('/') after each directory name.
     -q      Force printing of non-graphic characters as '?'.
     -R      Recursively list subdirectories encountered.
     -r      Reverse the order of the sort.
     -S      Sort by size, largest file first.
     -s      Display the number of file system blocks used by each file.
     -T      Display complete time information for the file.
     -t      Sort by time modified (most recent first).
     -u      Use file's last access time instead of last modification time.
     -x      Multi-column output sorted across rather than down.

     --help  Display this help message and exit.

EXAMPLES
     List files in long format:
       ls -l

     List all files including hidden ones:
       ls -a

     List files sorted by size:
       ls -S

     List files with human-readable sizes:
       ls -lh
`)
}

func parseArgs(args []string) []string {
	var files []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			files = append(files, arg)
			continue
		}

		// Handle combined flags like -la
		flags := arg[1:]
		for _, flag := range flags {
			switch flag {
			case '1':
				opts.One = true
			case 'a':
				opts.All = true
			case 'A':
				opts.AlmostAll = true
			case 'C':
				opts.Columns = true
			case 'c':
				opts.ChangeTime = true
			case 'd':
				opts.Directory = true
			case 'F':
				opts.Classify = true
			case 'f':
				opts.NoSort = true
				opts.All = true // -f implies -a
			case 'g':
				opts.GroupFormat = true
				opts.LongFormat = true
			case 'H':
				opts.NoFollow = true
			case 'h':
				opts.Human = true
			case 'i':
				opts.Inode = true
			case 'k':
				opts.Kilobytes = true
			case 'L':
				opts.Follow = true
			case 'l':
				opts.LongFormat = true
			case 'm':
				opts.Stream = true
			case 'n':
				opts.NumericFormat = true
				opts.LongFormat = true
			case 'o':
				opts.Flags = true
			case 'p':
				opts.Slash = true
			case 'q':
				opts.Quote = true
			case 'R':
				opts.Recursive = true
			case 'r':
				opts.Reverse = true
			case 'S':
				opts.SizeSort = true
			case 's':
				opts.Blocks = true
			case 'T':
				opts.FullTime = true
			case 't':
				opts.TimeSort = true
			case 'u':
				opts.AccessTime = true
			case 'x':
				opts.Comma = true
			}
		}
	}

	// Handle conflicting options
	if opts.LongFormat {
		opts.GroupFormat = opts.GroupFormat // -l overrides -g
	}

	if opts.NoSort {
		opts.TimeSort = false
		opts.SizeSort = false
	}

	return files
}

func processFiles(files []string) {
	var dirs, nonDirs []FileInfo

	// Separate directories from non-directories
	for _, file := range files {
		info, err := getFileInfo(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ls: %s: %v\n", file, err)
			continue
		}

		if info.IsDir && !opts.Directory {
			dirs = append(dirs, *info)
		} else {
			nonDirs = append(nonDirs, *info)
		}
	}

	// Sort and display non-directories first
	if len(nonDirs) > 0 {
		sortFiles(nonDirs)
		displayFiles(nonDirs, "")
	}

	// Process directories
	sortFiles(dirs)
	for i, dir := range dirs {
		if len(files) > 1 || opts.Recursive {
			if i > 0 || len(nonDirs) > 0 {
				fmt.Println()
			}
			fmt.Printf("%s:\n", dir.Name)
		}
		processDirectory(dir.Name)

		if opts.Recursive {
			processRecursive(dir.Name)
		}
	}
}

func processDirectory(dirPath string) {
	entries, err := readDirFast(dirPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ls: %s: %v\n", dirPath, err)
		return
	}

	// Filter entries
	var filtered []FileInfo
	for _, entry := range entries {
		if shouldSkipEntry(entry.Name) {
			continue
		}
		filtered = append(filtered, entry)
	}

	sortFiles(filtered)
	displayFiles(filtered, dirPath)
}

func readDirFast(dirPath string) ([]FileInfo, error) {
	file, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read directory entries in batches
	const batchSize = 1000
	var allEntries []FileInfo

	for {
		entries, err := file.Readdir(batchSize)
		if err != nil {
			if len(entries) == 0 {
				break
			}
		}

		if len(entries) == 0 {
			break
		}

		// Process entries concurrently
		infoChan := make(chan FileInfo, len(entries))

		for _, entry := range entries {
			pool.Submit(func(entry fs.FileInfo) func() {
				return func() {
					fullPath := filepath.Join(dirPath, entry.Name())
					info := convertFileInfo(entry, fullPath)
					infoChan <- *info
				}
			}(entry))
		}

		// Collect results
		for i := 0; i < len(entries); i++ {
			allEntries = append(allEntries, <-infoChan)
		}

		if err != nil {
			break
		}
	}

	return allEntries, nil
}

func getFileInfo(path string) (*FileInfo, error) {
	var stat syscall.Stat_t
	var err error

	if opts.Follow {
		err = syscall.Stat(path, &stat)
	} else {
		err = syscall.Lstat(path, &stat)
	}

	if err != nil {
		return nil, err
	}

	info := &FileInfo{
		Name:       filepath.Base(path),
		Mode:       fs.FileMode(stat.Mode),
		Size:       stat.Size,
		ModTime:    time.Unix(stat.Mtimespec.Sec, stat.Mtimespec.Nsec),
		AccessTime: time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec),
		ChangeTime: time.Unix(stat.Ctimespec.Sec, stat.Ctimespec.Nsec),
		Inode:      stat.Ino,
		Blocks:     stat.Blocks,
		Links:      uint64(stat.Nlink),
		Uid:        stat.Uid,
		Gid:        stat.Gid,
		IsDir:      (stat.Mode & syscall.S_IFMT) == syscall.S_IFDIR,
		IsSymlink:  (stat.Mode & syscall.S_IFMT) == syscall.S_IFLNK,
	}

	// Handle device files
	if (stat.Mode&syscall.S_IFMT) == syscall.S_IFCHR || (stat.Mode&syscall.S_IFMT) == syscall.S_IFBLK {
		info.Major = uint32(stat.Rdev >> 8)
		info.Minor = uint32(stat.Rdev & 0xff)
	}

	// Read symlink target
	if info.IsSymlink {
		if target, err := os.Readlink(path); err == nil {
			info.LinkTarget = target
		}
	}

	return info, nil
}

func convertFileInfo(entry fs.FileInfo, fullPath string) *FileInfo {
	info := &FileInfo{
		Name:    entry.Name(),
		Mode:    entry.Mode(),
		Size:    entry.Size(),
		ModTime: entry.ModTime(),
		IsDir:   entry.IsDir(),
	}

	// Get additional info via syscall for full compatibility
	if sysInfo := getSysInfo(fullPath); sysInfo != nil {
		if !sysInfo.AccessTime.IsZero() {
			info.AccessTime = sysInfo.AccessTime
		}
		if !sysInfo.ChangeTime.IsZero() {
			info.ChangeTime = sysInfo.ChangeTime
		}
		if sysInfo.Inode > 0 {
			info.Inode = sysInfo.Inode
		}
		if sysInfo.Blocks > 0 {
			info.Blocks = sysInfo.Blocks
		}
		if sysInfo.Links > 0 {
			info.Links = sysInfo.Links
		}
		info.Uid = sysInfo.Uid
		info.Gid = sysInfo.Gid
		info.Major = sysInfo.Major
		info.Minor = sysInfo.Minor
		info.IsSymlink = sysInfo.IsSymlink
		info.LinkTarget = sysInfo.LinkTarget
		info.Flags = sysInfo.Flags
	}

	return info
}

func getSysInfo(path string) *FileInfo {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err != nil {
		return nil
	}

	info := &FileInfo{
		AccessTime: time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec),
		ChangeTime: time.Unix(stat.Ctimespec.Sec, stat.Ctimespec.Nsec),
		Inode:      stat.Ino,
		Blocks:     stat.Blocks,
		Links:      uint64(stat.Nlink),
		Uid:        stat.Uid,
		Gid:        stat.Gid,
		IsSymlink:  (stat.Mode & syscall.S_IFMT) == syscall.S_IFLNK,
	}

	if (stat.Mode&syscall.S_IFMT) == syscall.S_IFCHR || (stat.Mode&syscall.S_IFMT) == syscall.S_IFBLK {
		info.Major = uint32(stat.Rdev >> 8)
		info.Minor = uint32(stat.Rdev & 0xff)
	}

	if info.IsSymlink {
		if target, err := os.Readlink(path); err == nil {
			info.LinkTarget = target
		}
	}

	return info
}

func shouldSkipEntry(name string) bool {
	if opts.All {
		return false
	}

	if opts.AlmostAll {
		return name == "." || name == ".."
	}

	return strings.HasPrefix(name, ".")
}

func sortFiles(files []FileInfo) {
	if opts.NoSort {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		a, b := files[i], files[j]

		var result bool

		if opts.TimeSort {
			var timeA, timeB time.Time
			if opts.AccessTime {
				timeA, timeB = a.AccessTime, b.AccessTime
			} else if opts.ChangeTime {
				timeA, timeB = a.ChangeTime, b.ChangeTime
			} else {
				timeA, timeB = a.ModTime, b.ModTime
			}
			result = timeA.After(timeB)
		} else if opts.SizeSort {
			result = a.Size > b.Size
		} else {
			result = strings.ToLower(a.Name) < strings.ToLower(b.Name)
		}

		if opts.Reverse {
			result = !result
		}

		return result
	})
}

func displayFiles(files []FileInfo, basePath string) {
	if opts.LongFormat || opts.GroupFormat || opts.NumericFormat {
		displayLongFormat(files)
	} else if opts.Stream {
		displayStreamFormat(files)
	} else if opts.Columns && !opts.One {
		displayColumnFormat(files)
	} else {
		displaySimpleFormat(files)
	}
}

func displayLongFormat(files []FileInfo) {
	// Calculate total blocks
	var totalBlocks int64
	for _, file := range files {
		totalBlocks += file.Blocks
	}

	if opts.Kilobytes {
		totalBlocks = (totalBlocks * BLOCKSIZE) / 1024
	}

	if len(files) > 0 {
		fmt.Printf("total %d\n", totalBlocks)
	}

	for _, file := range files {
		line := formatLongLine(file)
		fmt.Println(line)
	}
}

func formatLongLine(file FileInfo) string {
	var parts []string

	// Inode
	if opts.Inode {
		parts = append(parts, fmt.Sprintf("%8d", file.Inode))
	}

	// Blocks
	if opts.Blocks {
		blocks := file.Blocks
		if opts.Kilobytes && blocks > 0 {
			blocks = (blocks * BLOCKSIZE) / 1024
		}
		parts = append(parts, fmt.Sprintf("%6d", blocks))
	}

	// Mode
	parts = append(parts, formatMode(file.Mode, file.IsSymlink))

	// Links
	parts = append(parts, fmt.Sprintf("%3d", file.Links))

	// Owner
	if !opts.GroupFormat {
		if opts.NumericFormat {
			parts = append(parts, fmt.Sprintf("%-8d", file.Uid))
		} else {
			parts = append(parts, fmt.Sprintf("%-8s", getUserName(file.Uid)))
		}
	}

	// Group
	if opts.NumericFormat {
		parts = append(parts, fmt.Sprintf("%-8d", file.Gid))
	} else {
		parts = append(parts, fmt.Sprintf("%-8s", getGroupName(file.Gid)))
	}

	// Flags
	if opts.Flags {
		parts = append(parts, formatFlags(file.Flags))
	}

	// Size or device numbers
	if file.Major != 0 || file.Minor != 0 {
		parts = append(parts, fmt.Sprintf("%3d, %3d", file.Major, file.Minor))
	} else {
		sizeStr := formatSize(file.Size)
		parts = append(parts, fmt.Sprintf("%8s", sizeStr))
	}

	// Time
	timeStr := formatTime(file.ModTime, file.AccessTime, file.ChangeTime)
	parts = append(parts, timeStr)

	// Name
	name := file.Name
	if opts.Quote {
		name = quoteFileName(name)
	}

	if opts.Classify {
		name += getClassifyChar(file)
	} else if opts.Slash && file.IsDir {
		name += "/"
	}

	if file.IsSymlink && file.LinkTarget != "" {
		name += " -> " + file.LinkTarget
	}

	parts = append(parts, name)

	return strings.Join(parts, " ")
}

func formatMode(mode fs.FileMode, isSymlink bool) string {
	var buf [10]byte

	// File type
	switch mode & fs.ModeType {
	case fs.ModeDir:
		buf[0] = 'd'
	case fs.ModeSymlink:
		buf[0] = 'l'
	case fs.ModeNamedPipe:
		buf[0] = 'p'
	case fs.ModeSocket:
		buf[0] = 's'
	case fs.ModeDevice:
		buf[0] = 'b'
	case fs.ModeCharDevice:
		buf[0] = 'c'
	default:
		buf[0] = '-'
	}

	// Permissions
	perm := mode.Perm()

	// Owner permissions
	if perm&0400 != 0 {
		buf[1] = 'r'
	} else {
		buf[1] = '-'
	}
	if perm&0200 != 0 {
		buf[2] = 'w'
	} else {
		buf[2] = '-'
	}
	switch {
	case perm&0100 != 0 && mode&fs.ModeSetuid != 0:
		buf[3] = 's'
	case perm&0100 != 0:
		buf[3] = 'x'
	case mode&fs.ModeSetuid != 0:
		buf[3] = 'S'
	default:
		buf[3] = '-'
	}

	// Group permissions
	if perm&0040 != 0 {
		buf[4] = 'r'
	} else {
		buf[4] = '-'
	}
	if perm&0020 != 0 {
		buf[5] = 'w'
	} else {
		buf[5] = '-'
	}
	switch {
	case perm&0010 != 0 && mode&fs.ModeSetgid != 0:
		buf[6] = 's'
	case perm&0010 != 0:
		buf[6] = 'x'
	case mode&fs.ModeSetgid != 0:
		buf[6] = 'S'
	default:
		buf[6] = '-'
	}

	// Other permissions
	if perm&0004 != 0 {
		buf[7] = 'r'
	} else {
		buf[7] = '-'
	}
	if perm&0002 != 0 {
		buf[8] = 'w'
	} else {
		buf[8] = '-'
	}
	switch {
	case perm&0001 != 0 && mode&fs.ModeSticky != 0:
		buf[9] = 't'
	case perm&0001 != 0:
		buf[9] = 'x'
	case mode&fs.ModeSticky != 0:
		buf[9] = 'T'
	default:
		buf[9] = '-'
	}

	return string(buf[:])
}

func formatSize(size int64) string {
	if !opts.Human {
		return strconv.FormatInt(size, 10)
	}

	const (
		B  = 1
		KB = 1024 * B
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
		PB = 1024 * TB
		EB = 1024 * PB
	)

	switch {
	case size >= EB:
		return fmt.Sprintf("%.1fE", float64(size)/EB)
	case size >= PB:
		return fmt.Sprintf("%.1fP", float64(size)/PB)
	case size >= TB:
		return fmt.Sprintf("%.1fT", float64(size)/TB)
	case size >= GB:
		return fmt.Sprintf("%.1fG", float64(size)/GB)
	case size >= MB:
		return fmt.Sprintf("%.1fM", float64(size)/MB)
	case size >= KB:
		return fmt.Sprintf("%.1fK", float64(size)/KB)
	default:
		return strconv.FormatInt(size, 10)
	}
}

func formatTime(modTime, accessTime, changeTime time.Time) string {
	var t time.Time

	if opts.AccessTime {
		t = accessTime
	} else if opts.ChangeTime {
		t = changeTime
	} else {
		t = modTime
	}

	if opts.FullTime {
		return t.Format("Jan _2 15:04:05 2006")
	}

	now := time.Now()
	if now.Sub(t) < 6*30*24*time.Hour { // Less than 6 months
		return t.Format("Jan _2 15:04")
	}
	return t.Format("Jan _2  2006")
}

func getClassifyChar(file FileInfo) string {
	if file.IsDir {
		return "/"
	}
	if file.IsSymlink {
		return "@"
	}
	if file.Mode&0111 != 0 { // Executable
		return "*"
	}
	if file.Mode&fs.ModeNamedPipe != 0 {
		return "|"
	}
	if file.Mode&fs.ModeSocket != 0 {
		return "="
	}
	return ""
}

func displayStreamFormat(files []FileInfo) {
	var names []string
	for _, file := range files {
		name := file.Name
		if opts.Classify {
			name += getClassifyChar(file)
		}
		names = append(names, name)
	}
	fmt.Println(strings.Join(names, ", "))
}

func displayColumnFormat(files []FileInfo) {
	// Simple column display - can be optimized further
	for i, file := range files {
		name := file.Name
		if opts.Classify {
			name += getClassifyChar(file)
		}
		if opts.Inode {
			name = fmt.Sprintf("%8d %s", file.Inode, name)
		}
		if opts.Blocks {
			blocks := file.Blocks
			if opts.Kilobytes && blocks > 0 {
				blocks = (blocks * BLOCKSIZE) / 1024
			}
			name = fmt.Sprintf("%6d %s", blocks, name)
		}
		fmt.Printf("%-20s", name)
		if (i+1)%4 == 0 {
			fmt.Println()
		}
	}
	if len(files)%4 != 0 {
		fmt.Println()
	}
}

func displaySimpleFormat(files []FileInfo) {
	for _, file := range files {
		if opts.Inode {
			fmt.Printf("%8d ", file.Inode)
		}
		if opts.Blocks {
			blocks := file.Blocks
			if opts.Kilobytes && blocks > 0 {
				blocks = (blocks * BLOCKSIZE) / 1024
			}
			fmt.Printf("%6d ", blocks)
		}

		name := file.Name
		if opts.Quote {
			name = quoteFileName(name)
		}
		if opts.Classify {
			name += getClassifyChar(file)
		} else if opts.Slash && file.IsDir {
			name += "/"
		}

		fmt.Println(name)
	}
}

func processRecursive(dirPath string) {
	entries, err := readDirFast(dirPath)
	if err != nil {
		return
	}

	var subdirs []string
	for _, entry := range entries {
		if entry.IsDir && entry.Name != "." && entry.Name != ".." {
			if opts.All || !strings.HasPrefix(entry.Name, ".") {
				subdirs = append(subdirs, filepath.Join(dirPath, entry.Name))
			}
		}
	}

	for _, subdir := range subdirs {
		fmt.Printf("\n%s:\n", subdir)
		processDirectory(subdir)
		processRecursive(subdir)
	}
}

// Utility functions
var (
	userCache  = make(map[uint32]string)
	groupCache = make(map[uint32]string)
)

func getUserName(uid uint32) string {
	if name, ok := userCache[uid]; ok {
		return name
	}

	u, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil {
		userCache[uid] = strconv.FormatUint(uint64(uid), 10)
	} else {
		userCache[uid] = u.Username
	}
	return userCache[uid]
}

func getGroupName(gid uint32) string {
	if name, ok := groupCache[gid]; ok {
		return name
	}

	g, err := user.LookupGroupId(strconv.FormatUint(uint64(gid), 10))
	if err != nil {
		groupCache[gid] = strconv.FormatUint(uint64(gid), 10)
	} else {
		groupCache[gid] = g.Name
	}
	return groupCache[gid]
}

func formatFlags(flags uint32) string {
	// Simplified flag formatting - would need OS-specific implementation
	if flags == 0 {
		return "-"
	}

	var flagParts []string

	// OpenBSD flags
	if flags&0x00020000 != 0 { // UF_NODUMP
		flagParts = append(flagParts, "nodump")
	}
	if flags&0x00000020 != 0 { // SF_ARCHIVED
		flagParts = append(flagParts, "arch")
	}
	if flags&0x00000002 != 0 { // UF_APPEND
		flagParts = append(flagParts, "uappnd")
	}
	if flags&0x00000004 != 0 { // UF_IMMUTABLE
		flagParts = append(flagParts, "uchg")
	}
	if flags&0x00000010 != 0 { // SF_APPEND
		flagParts = append(flagParts, "sappnd")
	}
	if flags&0x00000008 != 0 { // SF_IMMUTABLE
		flagParts = append(flagParts, "schg")
	}

	if len(flagParts) == 0 {
		return "-"
	}
	return strings.Join(flagParts, ",")
}

func quoteFileName(name string) string {
	// Simple quote implementation - replace non-printable chars with ?
	var result strings.Builder
	for _, r := range name {
		if r < 32 || r > 126 {
			result.WriteByte('?')
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
