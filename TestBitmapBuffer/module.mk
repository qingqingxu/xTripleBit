local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,testBitmapBuffer,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
