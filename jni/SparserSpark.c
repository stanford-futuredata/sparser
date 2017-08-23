#include <jni.h>
#include <stdio.h>
#include "SparserSpark.h"
 
// Implementation of native method sayHello() of SparserSpark class
JNIEXPORT void JNICALL Java_SparserSpark_sayHello(JNIEnv *env, jobject thisObj) {
   printf("Hello World!\n");
   return;
}
