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
)


type Response_message struct {
	Result string
	Id int
	Error string
}

type Params_struct struct {
	Key string
	Rel string
	Value interface{}
}

type Config_file struct {
	ServerID int
	Protocol string
	IpAddress string
	Port int
	PersistentStorageContainer interface{}
	Methods []string
	Predecessor int
	Successor int

}

var config_obj Config_file
type Dict int
var filename string

//This function generates node ID using hash function on IP Address and Port
func get_node_ID() int{

	// config_obj.IpAddress
	return config_obj.Port % 32
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
    serverID := get_node_ID()
    config_obj.ServerID  = serverID
    


    fmt.Println("IP Address : ",config_obj.IpAddress)
    fmt.Println("IP Address : ",config_obj.Port)
    fmt.Println("ServerID : ",config_obj.ServerID)
    fmt.Println("Predecessor : ",config_obj.Predecessor)
    fmt.Println("Successor : ",config_obj.Successor)
    
    

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
}
//fix
func fix_my_successor() {
	    if config_obj.ServerID == 30 { //First node
    	config_obj.Successor = config_obj.ServerID
    	config_obj.Predecessor = config_obj.ServerID
    } else {
    	//call a known node
    	var my_successor int
    	c, err := jsonrpc.Dial("tcp","192.168.0.102:8222") //30th server
    	fmt.Println()
    	rpc_call := c.Go("Dict.Find_successor",&config_obj.ServerID,&my_successor,nil)		
					<-rpc_call.Done
					if err != nil {
						log.Fatal("Dict error:",err);
					}
					println("Reply received is : ",my_successor)
					config_obj.Successor = my_successor
    }
    fmt.Println("Successor : ",config_obj.Successor)


}
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
func (t *Dict) Find_successor(id *int,successor *int) error {

	if config_obj.ServerID == config_obj.Successor { //TO-DO: May have to change this.  (This is to cater for one node in the ring case)
		
		*successor = config_obj.ServerID
		config_obj.Successor = *id 
		
	}

	// if *id > config_obj.ServerID && *id < config_obj.Successor {
	// 	*successor = config_obj.Successor;
	// }

	return nil
}
//Search for triplet in the file, send appropriate reply
func (t *Dict) Lookup(args *string,reply *string) error {
	
	var f interface{}
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	var key string;var rel string;var id int
	input_obj := extract_params(f,&id)
	key = input_obj.Key
	rel = input_obj.Rel
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
	b,err := json.Marshal(resp_obj)
	*reply = string(b)  //Set the reply
	fmt.Println("Reply sent is ",string(b))
	return nil
	
}
//Insert the triplet if does not already exists and return true, else return false
func (t *Dict) Insert(args *string,reply *string) error {

	var f interface{}
	var write_file_reply int
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	
	var key string;var rel string;var id int
	input_obj := extract_params(f,&id)
	key = input_obj.Key
	rel = input_obj.Rel
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
		write_to_file(input_obj,&write_file_reply)
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
	
	b,err := json.Marshal(resp_obj)
	*reply = string(b)  //Set the reply
	fmt.Println("Reply sent is : ",string(b))
	// println("Done with the call..\n")
	return nil
}

//Read triplets from file, store them in list of Param_struct object.
//If any triplet matches with given key and relation, then update its value with new value
//In the end, write entire list back to the file
func (t* Dict) InsertOrUpdate(args *string,reply *string) error{
	
	var f interface{}
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	var key string;var rel string;var id int
	input_obj := extract_params(f,&id)
	key = input_obj.Key
	rel = input_obj.Rel
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
				file_obj.Value = input_obj.Value
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
    	write_to_file(input_obj,&write_file_reply)
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
    
    *reply = "update successful"
    // fmt.Println(*reply)
    return nil
}
//Read triplets from file, store them in list of Param_struct object.
//If any triplet matches with given key and relation, then do not add it to the list
//In the end, write entire list back to the file

func (t *Dict) Delete(args *string, reply *string) error {
	
	var f interface{}
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	var key string;var rel string;var id int
	input_obj := extract_params(f,&id)
	key = input_obj.Key
	rel = input_obj.Rel
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
    return nil	
}

//Read triplets from the file, store key, relation pair into the map and return the list of keys
func (t *Dict) ListKeys(args *string, reply *string) error {
	
	key_rel_map := make(map[string]string)
	var f interface{}
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	var id int
	extract_params(f,&id)
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
    *reply = string(b)
    fmt.Println("\nReply sent is :  ",string(b))
	// println("Done with the call..\n")
	return nil
}
//This function returns list of key,relation pair in paramater *reply
func (t *Dict) ListIDs(args *string, reply *string) error {
	
	key_rel_map := make(map[string]string)
	var f interface{}
	//Unmarshal in map of string to interface
	err := json.Unmarshal([]byte(*args), &f)
	if err != nil {
		log.Fatal("error:",err);
	}
	var id int
	extract_params(f,&id)
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
func (t *Dict) Shutdown(args *string,reply *string) error {
	
	fmt.Println("\nClosing the server!!")
	*reply = "shutdown successful!!"
	conn.Close()
	listener.Close()
	os.Exit(0)
	return nil
}

var listener net.Listener //this holds the Listener object
var conn net.Conn //This holds the connection
func startServer() {
	if len(os.Args) != 2{
 		fmt.Println("Specify server configuration file")
 		return
 	}

	read_server_config_file(os.Args[1])
    Dict := new(Dict)

    server := rpc.NewServer()
    server.Register(Dict)

    //Call find successor
   	fix_my_successor()
    var e error;var err error
    listener, e = net.Listen(config_obj.Protocol, ":"+strconv.Itoa(config_obj.Port))
    if e != nil {
        log.Fatal("listener error:", e)
        
    }
    println("\nAccepting connections...\n")
    for {
		
        conn,err  = listener.Accept()
        if err != nil {
            log.Fatal(err)
        }
		fmt.Println("\n*******New Connection established*******")        
        go server.ServeCodec(jsonrpc.NewServerCodec(conn))
    }
}

func main() {
    startServer()

}
