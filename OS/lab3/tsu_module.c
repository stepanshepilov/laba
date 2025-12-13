#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Steph Shepilov");
MODULE_DESCRIPTION("TSU OS Lab 1");
MODULE_VERSION("1.0");

#define MSG_LOAD "Welcome to the Tomsk State University\n"
#define MSG_UNLOAD "Tomsk State University forever!\n"

static int __init tsu_lab_load(void) 
{
    printk(KERN_INFO "%s", MSG_LOAD);
    return 0; 
}

static void __exit tsu_lab_unload(void) 
{
    printk(KERN_INFO "%s", MSG_UNLOAD);
}

module_init(tsu_lab_load);
module_exit(tsu_lab_unload);