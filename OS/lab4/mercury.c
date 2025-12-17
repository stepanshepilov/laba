#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/proc_fs.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/time.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Steph Shepilov");
MODULE_DESCRIPTION("Lab 4: Mercury in Leo Calculation");
MODULE_VERSION("1.0");

#define PROC_FILENAME "mercury_leo_time"
static struct proc_dir_entry *proc_entry = NULL;

#define MERCURY_PERIOD 88ULL
#define LEO_START_DAY  10ULL

static void calculate_days(char *buf, size_t max_len) {
    u64 now_sec = ktime_get_real_seconds();
    u64 days_total = now_sec / 86400; 
    u64 current_cycle_day = days_total % MERCURY_PERIOD;
    u64 days_left;

    if (current_cycle_day < LEO_START_DAY) {
        days_left = LEO_START_DAY - current_cycle_day;
    } else {
        days_left = (MERCURY_PERIOD - current_cycle_day) + LEO_START_DAY;
    }

    snprintf(buf, max_len, "Next Mercury in Leo in: %llu days\n", days_left);
}

static ssize_t mercury_proc_read(struct file *file_ptr, char __user *user_buf, 
                                 size_t buffer_len, loff_t *offset) {
    char kbuf[128];
    int len;
    
    if (*offset > 0) return 0;

    calculate_days(kbuf, sizeof(kbuf));
    len = strlen(kbuf);

    if (len > buffer_len) len = buffer_len;

    if (copy_to_user(user_buf, kbuf, len)) return -EFAULT;
        
    *offset += len;
    pr_info("Mercury module: read operation performed.\n");
    return len;
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 6, 0)
static const struct proc_ops proc_fops = {
    .proc_read = mercury_proc_read,
};
#else
static const struct file_operations proc_fops = {
    .read = mercury_proc_read,
};
#endif

static int __init mercury_init(void) {
    proc_entry = proc_create(PROC_FILENAME, 0644, NULL, &proc_fops);
    if (!proc_entry) {
        pr_err("Mercury module: failed to create /proc entry\n");
        return -ENOMEM;
    }
    pr_info("Mercury module: loaded successfully.\n");
    return 0;
}

static void __exit mercury_exit(void) {
    proc_remove(proc_entry);
    pr_info("Mercury module: unloaded.\n");
}

module_init(mercury_init);
module_exit(mercury_exit);