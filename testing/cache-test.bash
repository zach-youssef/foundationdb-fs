#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Make the test-mount directory
testdir=${SCRIPT_DIR}/test-mount
ls  ${testdir} > /dev/null 2>&1 || mkdir ${testdir}

# Make three directories to mount fuse client instances to
client1=${testdir}/client1
client2=${testdir}/client2
client3=${testdir}/client3

for dir in $client1 $client2 $client3; do
	ls $dir > /dev/null 2>&1 || mkdir $dir
done

# Compile Distribution
echo "Building client."
codedir=${SCRIPT_DIR}/../fslayer
${codedir}/gradlew installDist -p ${codedir} > /dev/null

# Save the name of the executable
execdir=${codedir}/app/build/install/app/bin/

trap "kill 0" EXIT

# Mount our 3 concurrent clients
echo "Mounting clients."
for dir in $client1 $client2 $client3; do
	printf "$dir\n$dir\n" | ${execdir}/app $dir > /dev/null 2>&1 &
done

# Create the file we will be testing the cache for
testfile=test-file
touch $client1/$testfile
chmod 666 $client1/$testfile

echo "Starting test..."

# Perform an initial write & read from each client so they have something
# in their cache
for dir in $client1 $client2 $client3; do
	echo Sup! >> $dir/$testfile
	cat $dir/$testfile > /dev/null
done

# Perform a flurry of writes to the end of the file, from each client
for i in {0..100}; do
	for dir in $client1 $client2 $client3; do
		echo -n $i >> $dir/$testfile
		cat $dir/$testfile > /dev/null
	done;
done;

# Validate that the clients all agree on what the file is
diff ${client1}/$testfile $client2/$testfile
if [ $? -ne 0 ]; then
	echo Client 1 does not match client 2! 1>2
	exit 1;
fi
diff ${client2}/$testfile $client3/$testfile
if [ $? -ne 0 ]; then
	echo Client 2 does not match client 3! 1>2
	exit 1;
fi

# Finish
cat <<BLOCK
Cache test succesful!
Clients agree on file value after 300 total writes.
BLOCK
exit 0;
