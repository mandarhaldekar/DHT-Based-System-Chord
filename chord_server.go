package main

import (
    
    "log"
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
	"encoding/json"
	"fmt"
	"os"
	"bufio"
	"strconv"
	"hash/fnv"
	"math"
	// "runtime"
	"sync"
	"time"
)


type Response_message struct {
	Result string
	Id int
	Error string
}

type Nodeid struct{
	IpAddress string `json:"ipAddress" bson:"ipAddress"`
	Port int `json:"port" bson:"port"`
	Id uint64 `json:"serverID" bson:"serverID"`
}

type Params_struct struct {
	Key string
	Rel string
	Value interface{}
}

type Config_file struct {
	ServerID uint64
	Protocol string
	IpAddress string
	Port int
	PersistentStorageContainer interface{}
	Methods []string
	Predecessor interface{}
	Successor interface{}


}

const max_bit int = 64
var finger = make([]Nodeid,max_bit+1)

var config_obj Config_file
type Dict int
var filename string
var successor Nodeid
var predecessor Nodeid

var selfnode Nodeid
//Hash function returns 64 bit
func hash(input string) uint32 {
        hashValue := fnv.New32()
        hashValue.Write([]byte(input))
        return hashValue.Sum32()
}





//This function generates node ID using hash function on IP Address and Port
func getHashValueForItem(s1 string,s2 string ) uint64{

	// config_obj.IpAddress
	a := uint64(hash(s1))
	b := uint64(hash(s2))
	c := uint64(0)
	d := uint64(0xFFFFFFFFFFFFFFFF)
	e := c | b
	f := (d | a) << 32
	g := e | f
	
	return g
	
}


//This function read server configuration from a file and stores informaton is Config_file structure
func read_server_config_file(severConfigFile string){

	file, err := os.Open(severConfigFile)

     if err != nil {
       panic(err.Error())
     }

     defer file.Close()
    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
    for scanner.Scan() {
		//unmarshal into Params_struct
		str_obj := scanner.Text()
		err := json.Unmarshal([]byte(str_obj),&config_obj)
		if err != nil{
			panic(err.Error())
		}
	
    }
    serverID := getHashValueForItem(config_obj.IpAddress,strconv.FormatInt(int64(config_obj.Port),10) )
    config_obj.ServerID  = serverID
    


    fmt.Println("IP Address : ",config_obj.IpAddress)
    fmt.Println("Port  : ",config_obj.Port)
    fmt.Println("ServerID : ",config_obj.ServerID)
    // fmt.Println("Predecessor : ",config_obj.Predecessor)
    // fmt.Println("Successor : ",config_obj.Successor)
    
    
	tempSuccessor := config_obj.Successor
    m1 := tempSuccessor.(map[string]interface{})
    // println("Successor is :")
    for k, v := range m1 {
    	 switch v.(type) {

    	 
    	     	 	
    	case string:
    		if k=="serverID"{
    			successor.Id,_ = strconv.ParseUint(v.(string), 10, 64)
    			// println("\nSuccessor ID is ");print(successor.Id)
    		} else{
				successor.IpAddress=v.(string)    	 	
				// println("\nSuccessor IP is ");print(successor.IpAddress)
    		}
    		break


	    default:
	    	successor.Port=int(v.(float64))
	    	// println("\nSuccessor Port ");print(successor.Port)
	    	break

	    }
    }
    tempPredecessor := config_obj.Predecessor
    m2 := tempPredecessor.(map[string]interface{})
    // println("\nPredecessor is : ")
    for k1, v1 := range m2 {
    	 switch v1.(type) {

    	 
    	     	 	
    	case string:
    		if k1=="serverID"{
    			predecessor.Id,_=strconv.ParseUint(v1.(string), 10, 64)
    			// println("\nPredecessor ID is ");print(predecessor.Id)
    		} else{
				predecessor.IpAddress=v1.(string)    	 	
				// println("\nPredecessor IP is ");print(predecessor.IpAddress)
    		}
    		break


	    default:
	    	predecessor.Port=int(v1.(float64))
	    	// println("\nPredecessor Port is ");print(predecessor.Port)
	    	break

	    }
    }

    persistenStorageContainerObj := config_obj.PersistentStorageContainer
    m := persistenStorageContainerObj.(map[string]interface{})
    for _, v := range m {
    	 switch vv := v.(type) {

	    default:
	    	filename = vv.(string)

	    }
    }

    if _, err := os.Stat(filename); err != nil {
		_, ferr := os.Create(filename)
		if ferr == nil {
    	fmt.Printf("File < " + filename + " > does not exit...creating file")
		}
	}

	for i:=max_bit; i>=1; i--{
		finger[i] = successor
	}
	
}

func fix_fingers() {

	fmt.Println("Printing fingerIDs")
	fmt.Println(max_bit)
	var i int
	for i = 1;i<=max_bit;i++{


	modID := ( config_obj.ServerID + uint64(math.Pow(2,float64(i-1))) % ( uint64(math.Pow(2,float64(64))) -1 ) )  
	var tempNodeid Nodeid
		//(*dict).Find_successor(modID,&tempNodeid)
	// println("Before calling find successor")
		tempNodeid = find_successor(modID)

		finger[i] = tempNodeid

		fmt.Printf("\n%dth entry : \t",i)
		fmt.Println(finger[i])

		// fmt.Println(modID)


	}
	

}
func find_successor(id uint64) Nodeid {

	
	if id<= successor.Id && id > config_obj.ServerID {
		
		return successor
	}else{
		var nextnode Nodeid
		nextnode = closest_preceding_node(id)
		if (nextnode==selfnode){
			return successor
		}
		// println("Next node: ");print(nextnode.IpAddress);println(nextnode.Port);println(nextnode.Id)
		var output Nodeid

		 c, err := jsonrpc.Dial(config_obj.Protocol, nextnode.IpAddress +":"+strconv.Itoa(nextnode.Port))
	

		 rpc_call := c.Go("Dict.Find_successor",id,&output,nil)		
		  <-rpc_call.Done
		 // println("output is : ");println(output.IpAddress);println(output.Port)
		 if err != nil {
		 	log.Fatal("Dict error:",err);
		 }		
		  return output
// return nextnode
	}	
	

}

func (t* Dict) Find_successor(id uint64,output *Nodeid) error {
	
	*output = find_successor(id)
	return nil
}
func closest_preceding_node(id uint64) Nodeid {
	
     for i:=max_bit; i>=1; i--{
    
		if finger[i].Id >config_obj.ServerID && finger[i].Id <id {
			return finger[i]
			
		}
	}
	
	return selfnode
	
	
}
// //fix
// func fix_my_successor() {
// 	    if config_obj.ServerID == 30 { //First node
//     	config_obj.Successor = config_obj.ServerID
//     	config_obj.Predecessor = config_obj.ServerID
//     } else {
//     	//call a known node
//     	var my_successor int
//     	c, err := jsonrpc.Dial("tcp","192.168.0.102:8222") //30th server
//     	fmt.Println()
//     	rpc_call := c.Go("Dict.Find_successor",&config_obj.ServerID,&my_successor,nil)		
// 					<-rpc_call.Done
// 					if err != nil {
// 						log.Fatal("Dict error:",err);
// 					}
// 					println("Reply received is : ",my_successor)
// 					config_obj.Successor = my_successor
//     }
//     fmt.Println("Successor : ",config_obj.Successor)


// }
//Write the object to persisten storage container
func write_to_file(file_obj Params_struct,reply *int) {

	*reply = 1;

	b, err := json.Marshal(file_obj)	
	//Create new file if does not exist	
	if _, err := os.Stat(filename); err != nil {
		_, ferr := os.Create(filename)
		if ferr == nil {
    	fmt.Printf("File does not exit...creating file")
		}
	}
	//Open file
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY,0600)
	
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if _, err = file.WriteString(string(b)+"\n"); err != nil {
		*reply = 1; //Failure
		panic(err)
	 } else {
		fmt.Printf("\nAppended into file\n")
		*reply = 0; //Success
	 }
}
//This function will extract key, relation and value from interface and store it in Param_struct object
func extract_params(f interface{},id *int) Params_struct {

	var input_obj Params_struct	
	m := f.(map[string]interface{})
	for _, v := range m {
	    switch vv := v.(type) {
		case string: //for method. No need here to extract
			break
		case int:break	    
	    case []interface{}: //for Params field, key, relation,value

			if len(vv) >= 2 {
				input_obj.Key = vv[0].(string)
				input_obj.Rel = vv[1].(string)
				if len(vv) == 3 {
					input_obj.Value = vv[2]
				}
			}

	    default: //for ID
	    	if vv != nil {
				*id = int(vv.(float64))
			}
	    }
	}
	    return input_obj
}
//This function will search the file to see if triplet is preset matching given key and relation
//It sets flag = 1 if the record is found, else set it to zero
func search_in_file(filestring string,key string,rel string,flag *int,str_obj *string) error {

	file, err := os.Open(filestring)

    if err != nil {
       panic(err.Error())
    }

    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	var file_obj Params_struct
	// var str_obj string
	*flag = 0
     for scanner.Scan() {
		//unmarshal into Params_struct
		*str_obj = scanner.Text()
		// fmt.Println("Json String is",*str_obj)
		err = json.Unmarshal([]byte(*str_obj),&file_obj)
		if err == nil{
			if file_obj.Key == key && file_obj.Rel == rel {
				fmt.Println("\nFound the record")
				*flag = 1
				break	
			}
			
		} else {
			panic(err.Error())
		}
		
		//fmt.Println(scanner.Text())
    }

	return nil
}

/**
This function finds the successor of id
*/
// func (t *Dict) Find_successor(id *int,successor *int) error {

// 	if config_obj.ServerID == config_obj.Successor { //TO-DO: May have to change this.  (This is to cater for one node in the ring case)
		
// 		*successor = config_obj.ServerID
// 		config_obj.Successor = *id 
		
// 	}

// 	// if *id > config_obj.ServerID && *id < config_obj.Successor {
// 	// 	*successor = config_obj.Successor;
// 	// }

// 	return nil
// }
//Search for triplet in the file, send appropriate reply
func (t *Dict) Lookup(input_objPtr *Params_struct,reply *string) error {
	
	// var f interface{}
	// //Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
	// 	log.Fatal("error:",err);
	// }
	var key string;var rel string;var id int
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	


	hashValue := getHashValueForItem(key, rel)
	//Find the successor node
	succ_node := find_successor(hashValue)

	fmt.Println("Hash value of the data :",hashValue)
	//Check if current node is successor node. If not then call RPC to insert at successor node

	if (succ_node.Id != config_obj.ServerID){
		fmt.Println("In IFFFF")
		// succ_node := find_successor(hashValue)
		fmt.Println("Successor node is ",succ_node.Port)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
	      var reply1 string

		 rpc_call := c.Go("Dict.Lookup",input_objPtr,&reply1,nil)		
		  <-rpc_call.Done

		  fmt.Println("If conf reply is : ",reply1)
		  *reply=reply1
		 // println("output is : ");println(output.IpAddress);println(output.Port)
		 if err != nil {
		 	log.Fatal("Dict error:",err);
		 }		

	} else { 


				var str_obj string
			flag := 0	
			search_in_file(filename,key,rel,&flag,&str_obj)
			var resp_obj *Response_message
			if flag == 1 {
				
				//Construct a reply message
				resp_obj = &Response_message{
								Result:str_obj,
								Id: id,
								Error: "null"}		
			} else {
				//Construct a reply message
				fmt.Println("\nNo matching record found")
				resp_obj = &Response_message{
								Result:"null",
								Id: id,
								Error: "null"}		
			}
			b,_ := json.Marshal(resp_obj)
			*reply = string(b)  //Set the reply
	}
	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel
	
	fmt.Println("Reply sent is ",*reply)
	return nil
	
}
//Insert the triplet if does not already exists and return true, else return false
func (t *Dict) Insert(input_objPtr *Params_struct,reply *string) error {

	// var f interface{}
	var write_file_reply int
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
		// log.Fatal("error:",err);
	// }
	
	var key string;var rel string;var id int
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel
	hashValue := getHashValueForItem(key, rel)

	//Find the successor node
	succ_node := find_successor(hashValue)

	fmt.Println("Hash value of the data :",hashValue)
	//Check if current node is successor node. If not then call RPC to insert at successor node
	if (succ_node.Id != config_obj.ServerID){
		fmt.Println("In IFFFF")
		succ_node := find_successor(hashValue)
		fmt.Println("Successor node is ",succ_node.Port)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
	      var reply1 string

		 rpc_call := c.Go("Dict.Insert",input_objPtr,&reply1,nil)		
		  <-rpc_call.Done

		  fmt.Println("If conf reply is : ",reply1)
		  *reply=reply1
		 // println("output is : ");println(output.IpAddress);println(output.Port)
		 if err != nil {
		 	log.Fatal("Dict error:",err);
		 }		

	} else { //Successor is current node, insert here
		fmt.Println("In else")
		var str_obj string
		flag := 0	

		search_in_file(filename,key,rel,&flag,&str_obj)
		var resp_obj *Response_message
		if flag == 1 {
			fmt.Println("Record already exits")
			resp_obj = &Response_message{
								Result:"false",
								Id: id,
								Error: "Record already exits"}		
		} else {
			write_to_file(*input_objPtr,&write_file_reply)
			if write_file_reply == 0 {
			
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"true",
								Id: id,
								Error: "null"}		
			} else {
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"false",
								Id: id,
								Error: "Error writing to file"}		
			}	
		}

		b,_:= json.Marshal(resp_obj)
		*reply = string(b)  //Set the reply
	}
	fmt.Println("Reply sent is : ",*reply)
	// println("Done with the call..\n")
	return nil

}

//Read triplets from file, store them in list of Param_struct object.
//If any triplet matches with given key and relation, then update its value with new value
//In the end, write entire list back to the file
func (t* Dict) InsertOrUpdate(input_objPtr *Params_struct,reply *string) error{
	
	// var f interface{}
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
	// 	log.Fatal("error:",err);
	// }
	var key string;var rel string
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel


	hashValue := getHashValueForItem(key, rel)

	//Find the successor node
	succ_node := find_successor(hashValue)

	fmt.Println("Hash value of the data :",hashValue)


	//Check if current node is successor node. If not then call RPC to insert at successor node
	if (succ_node.Id != config_obj.ServerID){
	
		fmt.Println("In IFFFF")
		succ_node := find_successor(hashValue)
		fmt.Println("Successor node is ",succ_node.Port)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
	      var reply1 string

		 rpc_call := c.Go("Dict.InsertOrUpdate",input_objPtr,&reply1,nil)		
		  <-rpc_call.Done

		  // fmt.Println("If conf reply is : ",reply1)
		  // *reply=reply1
		 // println("output is : ");println(output.IpAddress);println(output.Port)
		 if err != nil {
		 	log.Fatal("Dict error:",err);
		 }		

	} else { //Successor is current node, insertOrUpdate here	


			file, err := os.Open(filename)

		    if err != nil {
		       panic(err.Error())
		     }

		    

		    reader := bufio.NewReader(file)
		    scanner := bufio.NewScanner(reader)

		    scanner.Split(bufio.ScanLines)
			var file_obj Params_struct
			var str_obj string
			flag := 0
			list_of_file_obj := make([]Params_struct,0)

		    for scanner.Scan() {
				//unmarshal into Params_struct
				str_obj = scanner.Text()
				err = json.Unmarshal([]byte(str_obj),&file_obj)
				if err == nil{
					if file_obj.Key == key && file_obj.Rel == rel {
						fmt.Println("\nFound the record")
						file_obj.Value = (*input_objPtr).Value
						flag = 1	
					}
					
				} else {
					panic(err.Error())
				}
				list_of_file_obj = append(list_of_file_obj,file_obj)
				//fmt.Println(scanner.Text())
		    }
		    file.Close()
		    if flag == 0 {
		    	//Insert in the end
		    	write_file_reply := 1
		    	write_to_file((*input_objPtr),&write_file_reply)
		    	if write_file_reply == 0 {
		    		fmt.Println("Record does not exist..Inserting")
		    	} else {
		    		fmt.Println("Error while inserting")
		    	}
		    	
		    } else {
		   		fmt.Println("Updating the record")
		    	file, _ := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC,0600)
		    	
		    	for _, v := range list_of_file_obj {
		 			b, err := json.Marshal(v)
		 			if _, err = file.WriteString(string(b)+"\n"); err != nil {
					panic(err)
			 		}

				}
				file.Close()
		  	  }
  	} //else of successor
    
    *reply = "update successful"
    // fmt.Println(*reply)
    return nil
}
//Read triplets from file, store them in list of Param_struct object.
//If any triplet matches with given key and relation, then do not add it to the list
//In the end, write entire list back to the file

func (t *Dict) Delete(input_objPtr *Params_struct, reply *string) error {
	
	// var f interface{}
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
	// 	log.Fatal("error:",err);
	// }
	var key string;var rel string;
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel


	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel


	hashValue := getHashValueForItem(key, rel)

	//Find the successor node
	succ_node := find_successor(hashValue)

	fmt.Println("Hash value of the data :",hashValue)


	//Check if current node is successor node. If not then call RPC to insert at successor node
	if (succ_node.Id != config_obj.ServerID){
	
		fmt.Println("In IFFFF")
		succ_node := find_successor(hashValue)
		fmt.Println("Successor node is ",succ_node.Port)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
	      var reply1 string

		 rpc_call := c.Go("Dict.Delete",input_objPtr,&reply1,nil)		
		  <-rpc_call.Done

		  // fmt.Println("If conf reply is : ",reply1)
		  // *reply=reply1
		 // println("output is : ");println(output.IpAddress);println(output.Port)
		 if err != nil {
		 	log.Fatal("Dict error:",err);
		 }		

	} else { //Successor is current node, delete here	



			file, err := os.Open(filename)

		    if err != nil {
		       panic(err.Error())
		     }
		    

		    reader := bufio.NewReader(file)
		    scanner := bufio.NewScanner(reader)

		    scanner.Split(bufio.ScanLines)
			var file_obj Params_struct
			var str_obj string
			
			list_of_file_obj := make([]Params_struct,0)

		    for scanner.Scan() {
				//unmarshal into Params_struct
				str_obj = scanner.Text()
				err = json.Unmarshal([]byte(str_obj),&file_obj)
				if err == nil {

					if file_obj.Key == key && file_obj.Rel == rel {
						fmt.Println("\nFound the record...Deleting")
						
					} else {
						list_of_file_obj = append(list_of_file_obj,file_obj)		
					}
					
				} else {
					panic(err.Error())
				}
				
				//fmt.Println(scanner.Text())
		    }
		    file.Close()
		   
			//Write list to the file
			fileptr, _ := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC,0600)
			for _, v := range list_of_file_obj {
					b, err := json.Marshal(v)
					if _, err = fileptr.WriteString(string(b)+"\n"); err != nil {
				panic(err)
		 		}

			}
			fileptr.Close() 
		    

		    *reply = "Delete successful"
	} //else of successor
    return nil	
}

func getListOfKeys() string {

	key_rel_map := make(map[string]string)
	var reply string
	var id int
	// extract_params(f,&id)
	file, err := os.Open(filename)

    if err != nil {
       panic(err.Error())
     }
    

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	var file_obj Params_struct
	var str_obj string

    for scanner.Scan() {
		//unmarshal into Params_struct
		str_obj = scanner.Text()
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil {
			//Add to map
			key_rel_map[file_obj.Key] = file_obj.Rel
			
		} else {
			panic(err.Error())
		}
		
		//fmt.Println(scanner.Text())
    }
    file.Close()
    list_of_keys := make([]string,0)
    for k, _ := range key_rel_map {
    	list_of_keys = append(list_of_keys,k)
    }
    //Marshal the array to convert to string
    string_arr,_ := json.Marshal(list_of_keys)

    resp_obj := &Response_message{
						Result:string(string_arr),
						Id: id,
						Error: "null"}	

    b,_ := json.Marshal(resp_obj)
    reply = string(b)
    fmt.Println("\nReply sent is :  ",string(b))

	// println("Done with the call..\n")
	return reply
}

//Read triplets from the file, store key, relation pair into the map and return the list of keys
func (t *Dict) ListKeys(input_objPtr *Params_struct, reply *string) error {
	
	
	fmt.Println("Reply string before calling the function is : ",*reply)
	

	var key string;
	key = (*input_objPtr).Key
	println("Key is : ",key)
	hashValue := getHashValueForItem(config_obj.IpAddress, strconv.Itoa( config_obj.Port))

	if(len(key) == 0){ //First node initiating request insert its own ID in key and make RPC to Successor

		fmt.Println("First node initiating listKeys ")	

		list_of_keys_reply := getListOfKeys()
		(*input_objPtr).Key = strconv.FormatUint(config_obj.ServerID,10)

		//

		//PRC to sucessor
		succ_node := find_successor(hashValue)

		if(succ_node.Id != config_obj.ServerID) { //node is successor of itself, one node case
			fmt.Println("Successor node is ",succ_node.Port)

			c, _ := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
		       var reply1 string
		     

			 rpc_call := c.Go("Dict.ListKeys",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
			 ( *reply) += list_of_keys_reply +reply1
			  fmt.Println("Reply string after RPC to next node is : ",*reply)

		} else {
			*reply = list_of_keys_reply
			return nil	
		}

		

	} else {

		id, err := strconv.ParseUint(key, 10, 64)
		
		if err != nil {
    		panic(err)	
		}
		if(id != config_obj.ServerID){ //Checking if it is the first node
			//make an RPC call
			
			list_of_keys_reply := getListOfKeys()


			//PRC to sucessor
			succ_node := find_successor(hashValue)
			fmt.Println("Successor node is ",succ_node.Port)
			c, _ := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
		       var reply1 string
		  

			 rpc_call := c.Go("Dict.ListKeys",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
		(*reply) += list_of_keys_reply+reply1
			  fmt.Println("Reply string after RPC to next node is : ",*reply)


		}else{
			(*reply)=""
		} 


	}
	return nil
	
}
//This function returns list of key,relation pair in paramater *reply
func (t *Dict) ListIDs(input_objPtr *Params_struct, reply *string) error {
	
	key_rel_map := make(map[string]string)
	// var f interface{}
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
		// log.Fatal("error:",err);
	// }
	var id int
	// extract_params(f,&id)
	file, err := os.Open(filename)

    if err != nil {
       panic(err.Error())
     }
    

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	var file_obj Params_struct
	var str_obj string

    for scanner.Scan() {
		//unmarshal into Params_struct
		str_obj = scanner.Text()
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil {
			//Add to map
			key_rel_map[file_obj.Key] = file_obj.Rel
			
		} else {
			panic(err.Error())
		}
		
		//fmt.Println(scanner.Text())
    }
    file.Close()
    //two dimensional array tp hold key relation pair
    list_of_keys := make([][]string,0)
    for k, v := range key_rel_map {
    	key_rel_pair := []string{k,v}
    	
    	list_of_keys = append(list_of_keys,key_rel_pair)
    }
    //Marshal the array to convert to string
    string_arr,_ := json.Marshal(list_of_keys)
    resp_obj := &Response_message{
						Result:string(string_arr),
						Id: id,
						Error: "null"}	

    b,_ := json.Marshal(resp_obj)
    *reply = string(b)
    fmt.Println("\nReply sent is : ",string(b))
	// println("Done with the call..\n")
	return nil
}
//This function closes the existing connection, stops the listener and then exit the server program
func (t *Dict) Shutdown(input_objPtr *Params_struct,reply *string) error {
	
	fmt.Println("\nClosing the server!!")
	*reply = "shutdown successful!!"
	conn.Close()
	listener.Close()
	os.Exit(0)
	return nil
}


var listener net.Listener //this holds the Listener object
var conn net.Conn //This holds the connection
var dict *Dict
func startServer() {
	if len(os.Args) != 2{
 		fmt.Println("Specify server configuration file")
 		return
 	}

	read_server_config_file(os.Args[1])
    dict = new(Dict)
  
    server := rpc.NewServer()
    server.Register(dict)
     
	selfnode.Id=config_obj.ServerID
	selfnode.Port=config_obj.Port
	selfnode.IpAddress=config_obj.IpAddress
    //Call find successor
   	// fix_my_successor()
    var e error;var err error
    listener, e = net.Listen(config_obj.Protocol, ":"+strconv.Itoa(config_obj.Port))

    if e != nil {
        log.Fatal("listener error:", e)
        
    }
    println("\nAccepting connections...\n")
    
   	
    	
    for {
		    
 
    //     defer wg.Done()
		      // println("in for loop")
        	conn,err  = listener.Accept()
        	if err != nil {
        		// println("Connection is ",conn)
        	    log.Fatal(err)

    	    }

    	    fmt.Println("\n*******New Connection established*******")        
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
       }
		
       

}

func main() {
	// runtime.GOMAXPROCS(2)
	var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
    startServer()
    }()

    var input string
	fmt.Scanln(&input)
	fmt.Println("Done")    

    go func() {
      defer wg.Done()
	  println("\nfinger thread executing")        
      time.Sleep(1000* time.Microsecond)
      fix_fingers()
    }()
    wg.Wait()

	

}
