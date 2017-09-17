package mapreduce

import (
	"sort"
	"os"
	"log"
	"encoding/json"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	/*
		该函数收集由`map`产生的nReduce文件(f-*-), 然后对这些文件运行reduce函数. 最终产生nReduce个结果文件
		1. 从nMap个中间文件中解码Json数据, 使用map收集数据
		2. 对key-values进行排序
		3. 调用reduce函数
		4. 最后生成nReduce个结果文件
	 */
	debug("DEBUG: Reduce jobName: %v, reduceTaskNumber: %v, nMap: %s\n", jobName, reduceTaskNumber, nMap)
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		debug("Reduce fileName: %s\n", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Open Error: ", fileName)
		}
		// func NewDecoder(r io.Reader) *Decoder
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break  // 此时文件解码完毕
			}
			_, ok := keyValues[kv.Key]
			if !ok { // 说明当前并没有这个key
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
		file.Close()
	}

	var keys []string
	for k, _ := range keyValues {  //value是空字符串数组
		//fmt.Printf("key: %s, ", k)
		keys = append(keys, k)
	}
	sort.Strings(keys)  // 递增排序
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	debug("DEBUG: mergeFileName: %v\n", mergeFileName)
	file, err := os.Create(mergeFileName)
	if err != nil {
		log.Fatal("Create file error: ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		enc.Encode(&KeyValue{k, res})  // 注意此处要取地址
	}
	file.Close()
}
