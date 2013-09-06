blueprints-ramcloud-graph
=========================

A TinkerPop Blueprints implementation on top of RAMCloud

Setup
=====
1. Copy src/main/java/edu/stanford/ramcloud/JRamCloud.java and
src/main/cpp/edu_stanford_ramcloud_JRamCloud.cc to your
ramcloud/bindinds/java/edu/stanford/ramcloud directory, overwriting what is
already there.

2. Generate the C++ header files containing the function signatures for all the
native methods in the Java RamCloud library:

javah -cp ../../../ edu.stanford.ramcloud.JRamCloud

3. Compile the ramcloud C++ library (assuming ramcloud is in your ${HOME}
directory and you have already compiled ramcloud):

c++ -Wall -O3 -shared -fPIC -std=c++0x \
  -I/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.9.x86_64/include/ \
  -I/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.9.x86_64/include/linux/ \
  -I${HOME}/ramcloud/src/ -I${HOME}/ramcloud/obj.master/ \
  -I${HOME}/ramcloud/logcabin/ -I${HOME}/ramcloud/gtest/include/ \
  -L${HOME}/ramcloud/obj.master -o libedu_stanford_ramcloud_JRamCloud.so \
  edu_stanford_ramcloud_JRamCloud.cc -lramcloud

4. Update LD_LIBRARY_PATH (assuming ramcloud is in your ${HOME} directory) to
include the library and also any other ramcloud libraries:

export
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${HOME}/ramcloud/bindings/java/edu/stanford/ramcloud:${HOME}/ramcloud/obj.master

5. Startup a ramcloud cluster somewhere and modify RamCloudGraph.java to point
to the coordinator.

6. Compile this package (blueprints-ramcloud-graph) using maven and run :)
