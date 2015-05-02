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
	"runtime"
	"sync"
	"time"
	"regexp"
)


type Response_message struct {
	Result string
	Error string
}

type Nodeid struct{
	IpAddress string `json:"ipAddress" bson:"ipAddress"`
	Port int `json:"port" bson:"port"`
	Id int `json:"serverID" bson:"serverID"`
}

type Params_struct struct {
	Key string
	Rel string
	Value interface{}
	// Permission string
}

type Params_struct_with_perm struct {
	Key string
	Rel string
	Value interface{}
	Permission string
}

type Config_file struct {
	ServerID int
	Protocol string
	IpAddress string
	Port int
	PersistentStorageContainer interface{}
	Methods []string
	Predecessor interface{}
	Successor interface{}
    Knownnode interface{}
    TimetoLive int64
    IsFirstNode int

}

const max_bit int = 8
var finger = make([]Nodeid,max_bit+1)

var config_obj Config_file
type Dict int
var filename string
var successor Nodeid
var predecessor Nodeid
var knownnode Nodeid
var selfnode Nodeid
//Hash function returns 64 bit
func hash(input string) uint32 {
        hashValue := fnv.New32()
        hashValue.Write([]byte(input))
        return ((hashValue.Sum32()) % 15)
}





//This function generates node ID using hash function on IP Address and Port
//This function expects key as first input and relation as second and returns HashValue where 
//first 4 bits corresponds to key and last 4 bits corresponds to relation
func getHashValueForItem(s1 string,s2 string ) int{


	// config_obj.IpAddress
	a := hash(s1)
	b := hash(s2)
	d := a<<4
	c := d | b
	return int(c)
	
}

func removebackslash(s string) string {
    var tempstr string
        reg, err := regexp.Compile("\\\\")
        if err != nil {
                log.Fatal(err)
        }
        tempstr = reg.ReplaceAllString(s,"")
        
    return tempstr
}


func Join(){
	
	var dummy *Nodeid
	dummy = &Nodeid{
				IpAddress:"",
				Port:-1,
				Id:-1}
    config_obj.Predecessor=*dummy
    predecessor=*dummy
    c, _ := jsonrpc.Dial(config_obj.Protocol, knownnode.IpAddress +":"+strconv.Itoa(knownnode.Port))
	defer c.Close()
	      //var reply1 string
          var id int
          id=config_obj.ServerID
          var succ Nodeid
		 rpc_call := c.Go("Dict.Find_successor",id,&succ,nil)	

		  <-rpc_call.Done
		  config_obj.Successor=succ
		  successor=succ
		  print ("In join: Changed Successor to:", successor.Id )
// myData:= make([]Params_struct,0)
var myData string
c1, _ := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
defer c1.Close()
rpc_call = c1.Go("Dict.Retrieve_data",&id,&myData,nil)	

		  <-rpc_call.Done
// writeListOfObjectsToFile(myData)

file, err := os.OpenFile(filename, os.O_TRUNC|os.O_WRONLY,0600)
	
	if err != nil {
		panic(err)
	}
	defer file.Close()

	if _, err = file.WriteString(myData); err != nil {
		
		panic(err)
	 } 

fmt.Println("My data")
fmt.Println(myData)
	fmt.Println("Retrieved my data")
}


func (t* Dict) Retrieve_data(id *int,myData *string) error{

file, err := os.Open(filename)
	list_of_file_obj := make([]Params_struct,0)

    if err != nil {
       panic(err.Error())
    }

    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)
	
    scanner.Split(bufio.ScanLines)
	 *myData=""
	// var str_obj string
var flag int
flag=0

     for scanner.Scan() {
		//unmarshal into Params_struct
		var file_obj Params_struct
		str_obj := scanner.Text()
		 fmt.Println("Json String is",str_obj)
		err=json.Unmarshal([]byte(str_obj),&file_obj)
		if err != nil {
       panic(err.Error())
    }
	//	if err == nil{
			fmt.Println("\n\n\n\n\n\n\n\n\n\n\n")
					fmt.Println("Hash", getHashValueForItem(file_obj.Key,file_obj.Rel))
if  ((getHashValueForItem(file_obj.Key,file_obj.Rel) <= *id ) && (*id < config_obj.ServerID)) ||( config_obj.ServerID < *id && (getHashValueForItem(file_obj.Key,file_obj.Rel) < *id)  ) {
								
					fmt.Println("\n\n\n\n\n\n\n\n\n\n\n")
					fmt.Println("Hash", getHashValueForItem(file_obj.Key,file_obj.Rel))
					if(flag==1)	{
						*myData+="\n"
					}
				*myData+=str_obj
				flag=1
				// myData=append(myData,file_obj)
				// (*myData)+=str_obj
			}else{
				list_of_file_obj=append(list_of_file_obj,file_obj)

			}
			
		// } else {
		// 	panic(err.Error())
		// }
		
		
	
    }
  
    writeListOfObjectsToFile(list_of_file_obj)

	return nil

}


func Stabilize() error{

	 c, _:= jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
	 
    defer c.Close()

    a:=0
    var pred Nodeid
		 rpc_call := c.Go("Dict.AskPredecessor",&a,&pred,nil)

		 <-rpc_call.Done
		 fmt.Println("Successor was:::::::::",successor.Id,successor.Port)
		 fmt.Println("Returned predecessor is:::::::::",pred.Id,pred.Port)
		 //Last condition is to handle circular nature of ring
		 if ((!(pred.Id==-1))&&((pred.Id > config_obj.ServerID && pred.Id < successor.Id) || (successor.Id==selfnode.Id) || (successor.Id < config_obj.ServerID && pred.Id != config_obj.ServerID)) ) {
		 	fmt.Println("In If condition to update Successor")
		 	config_obj.Successor=pred
		 	successor=pred
		 	fmt.Printf("Updated successor %d , IP is %s , Port is %d ",successor.Id,successor.IpAddress,successor.Port)


         }
         fmt.Println("in stabilize before calling notify")
        c1, e1 := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
        
		if e1 != nil {
        	fmt.Println("Error while calling RPC")
        	panic(e1)
        }
		defer c1.Close()

        rpc_call1 := c1.Go("Dict.Notify",&selfnode,&a,nil)

		 <-rpc_call1.Done

fmt.Println("in stabilize after notify returned")
    time.Sleep(5 * time.Second)
    timeVar := time.Now().In(time.UTC)
	fmt.Println("---This is the Stabilize---",timeVar.Format("20060102150405"))
	fmt.Println("Successor is : ",successor.Id)
	fmt.Println("Predecessor is : ",predecessor.Id)
fmt.Println("leaving stabilize")
return nil
}


func (t* Dict) Notify(newnode *Nodeid,b *int) error{
	
	fmt.Println("In Notify from ID : ",(*newnode).Id)
	var dummy *Nodeid
	dummy = &Nodeid{
				IpAddress:"",
				Port:-1,
				Id:-1}

	//Last condition is to handle case where predecessor is greater than current node ID e.g. 13 is predecessor of 1 and node zero joins
				
	 if ((predecessor.Id==(*dummy).Id ) ||  ((*newnode).Id > predecessor.Id && (*newnode).Id <config_obj.ServerID) || (predecessor.Id==selfnode.Id) || predecessor.Id > config_obj.ServerID)  {
// if ((*newnode).Id > predecessor.Id && (*newnode).Id <config_obj.ServerID)  {
		config_obj.Predecessor=*newnode
		predecessor=*newnode
		fmt.Println("I changed my Predecessor to : ",(*newnode).Id)
	}


	*b = 0
	return nil
}


func (t* Dict) AskPredecessor(a int,prednode *Nodeid) error{
*prednode=predecessor

return nil
}

func CheckPredecessor() error{
	timeVar1 := time.Now().In(time.UTC)
	fmt.Println("In check pred before", timeVar1.Format("20060102150405"))
	_, err := net.DialTimeout("tcp", predecessor.IpAddress +":"+strconv.Itoa(predecessor.Port),time.Duration(15)*time.Second)
	timeVar1 = time.Now().In(time.UTC)
	fmt.Println("In check pred after", timeVar1.Format("20060102150405"))
	var dummy *Nodeid
		dummy = &Nodeid{
					IpAddress:"",
					Port:-1,
					Id:-1}			
	if err!= nil {
	config_obj.Predecessor=(*dummy)
	predecessor=(*dummy)
    fmt.Println("assigning dummy to predecessor", err)

	}


    time.Sleep(5* time.Second)
    timeVar := time.Now().In(time.UTC)
	fmt.Println("---This is CheckPredecessor---",timeVar.Format("20060102150405"))
return nil
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
    fmt.Println("TimetoLive : ",config_obj.TimetoLive)
    // fmt.Println("Predecessor : ",config_obj.Predecessor)
    // fmt.Println("Successor : ",config_obj.Successor)


    Time_to_live=time.Duration(config_obj.TimetoLive)*time.Minute

	//Time_to_live=config_obj.TimetoLive*time.Minute

    selfnode.Id=config_obj.ServerID
	selfnode.Port=config_obj.Port
	selfnode.IpAddress=config_obj.IpAddress

    successor = selfnode
    fmt.Println("*****SUCCESSOR***********")
	fmt.Println("ID : ",successor.Id)
	fmt.Println("IP : ",successor.IpAddress)
	fmt.Println("Port : ",successor.Port)    

	predecessor = selfnode
	fmt.Println("*********PREDECSSOR******")
	fmt.Println("ID : ",predecessor.Id)
	fmt.Println("IP ",predecessor.IpAddress)
	fmt.Println("Port : ",predecessor.Port)    
    
    
	// tempSuccessor := config_obj.Successor
 //    m1 := tempSuccessor.(map[string]interface{})
 //    // println("Successor is :")
 //    for k, v := range m1 {
 //    	 switch v.(type) {

    	 
    	     	 	
 //    	case string:
 //    		if k=="serverID"{
 //    			temp_id,_ := strconv.ParseInt(v.(string), 10, 32)
 //    			successor.Id = int(temp_id)
 //    			// println("\nSuccessor ID is ");print(successor.Id)
 //    		} else{
	// 			successor.IpAddress=v.(string)    	 	
	// 			// println("\nSuccessor IP is ");print(successor.IpAddress)
 //    		}
 //    		break


	//     default:
	//     	successor.Port=int(v.(float64))
	//     	// println("\nSuccessor Port ");print(successor.Port)
	//     	break

	//     }
 //    }



tempknownnode := config_obj.Knownnode
    m3 := tempknownnode.(map[string]interface{})
    // println("Successor is :")
    for k, v := range m3 {
    	 switch v.(type) {

    	 
    	     	 	
    	case string:
    		if k=="serverID"{
    			
    			temp_id,_ := strconv.ParseInt(v.(string), 10, 32)
    			knownnode.Id = int(temp_id)
    			println("\nKnownnode ID is ");print(knownnode.Id)
    		} else{
				knownnode.IpAddress=v.(string)    	 	
				println("knownnode IP is ");print(knownnode.IpAddress)
    		}
    		break


	    default:
	    	knownnode.Port=int(v.(float64))
	    	println("knownnode Port ");print(knownnode.Port)
	    	break

	    }
    }


    // tempPredecessor := config_obj.Predecessor
    // m2 := tempPredecessor.(map[string]interface{})
    // // println("\nPredecessor is : ")
    // for k1, v1 := range m2 {
    // 	 switch v1.(type) {

    	 
    	     	 	
    // 	case string:
    // 		if k1=="serverID"{
    			
    // 			temp_id,_ := strconv.ParseInt(v1.(string), 10, 32)
    // 			predecessor.Id = int(temp_id)
    // 			// println("\nPredecessor ID is ");print(predecessor.Id)
    // 		} else{
				// predecessor.IpAddress=v1.(string)    	 	
				// // println("\nPredecessor IP is ");print(predecessor.IpAddress)
    // 		}
    // 		break


	   //  default:
	   //  	predecessor.Port=int(v1.(float64))
	   //  	// println("\nPredecessor Port is ");print(predecessor.Port)
	   //  	break

	   //  }
    // }

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


	modID := ( (config_obj.ServerID + int(math.Pow(2,float64(i-1)))) % ( int(math.Pow(2,float64(8))) -1 ) )  
	//fmt.Println("id to find::::::", modID)
	var tempNodeid Nodeid
		//(*dict).Find_successor(modID,&tempNodeid)
	// println("Before calling find successor")
		tempNodeid = find_successor(modID)

		finger[i] = tempNodeid

		fmt.Printf("\n%dth entry : \t",i)
		fmt.Println(finger[i])

		// fmt.Println(modID)


	}
	time.Sleep(5* time.Second)
    timeVar := time.Now().In(time.UTC)
	fmt.Println("---This is fix_fingers---",timeVar.Format("20060102150405"))


}
func find_successor(id int) Nodeid {

	
	if id<= successor.Id && id > config_obj.ServerID {
		
		return successor
	}else{
		var nextnode Nodeid
		nextnode = closest_preceding_node(id)
		fmt.Printf("Next node port : %d   ID : %d",nextnode.Port,nextnode.Id)
		if (nextnode==selfnode){
			if( (id<successor.Id  || id >config_obj.ServerID) && (successor.Id <=  config_obj.ServerID)){
				return successor
			}else if(id<= config_obj.ServerID){
		
			fmt.Println("Returning self node ... for data value ",id)
			 

			 return selfnode
			}else{
				nextnode = successor
			}
		}
		// println("Next node: ");print(nextnode.IpAddress);println(nextnode.Port);println(nextnode.Id)
		// if (nextnode==selfnode){
		// 	if( id<successor.Id || id > config_obj.ServerID){
		// 		return successor
		// 	}else if(id< config_obj.ServerID){
		
		// 	// return selfnode
		// 	 return selfnode
		// 	}
		// }
		var output Nodeid

		 c, err := jsonrpc.Dial(config_obj.Protocol, nextnode.IpAddress +":"+strconv.Itoa(nextnode.Port))
		defer c.Close()

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

func (t* Dict) Find_successor(id int,output *Nodeid) error {
	
	*output = find_successor(id)
	return nil
}

func closest_preceding_node(id int) Nodeid {
	
     for i:=max_bit; i>=1; i--{
    
    	//To handle circular cases. 1st condition is normal condition. Second is when finger is less than id to be searched. and third is when finger is greater than Id to be searched. 
		if (finger[i].Id >config_obj.ServerID && finger[i].Id <id) ||(id<config_obj.ServerID &&finger[i].Id <id) || (finger[i].Id > config_obj.ServerID && id < config_obj.ServerID){
			return finger[i]
			
		}
	}
	
	return selfnode
	
	
}// //fix
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


//This is structure uses at Value field for triplet
type ValueType_struct struct {
	Content interface{}
	Size string
	Created string
	Modified string
	Accessed string
	Permission string
	
}

func getSizeInBytes(val interface{}) string {
     s,_:=json.Marshal(&val)
     // fmt.Println("BATMAN",s)

	 return strconv.FormatInt(int64(len(s)),10)		


}
func getPermission(file_obj Params_struct) string{
	val := file_obj.Value
	var permission string
	//var contentVal interface{}
	// var modValue interface{}
    m1 := val.(map[string]interface{})
    // println("Successor is :")
    for k, v := range m1 {
    	 switch v.(type) {

    	 
    	     	 	
		case interface{}:
			if k=="Permission" {	
    			
    			permission = v.(string)
    		}
   
			break 	     	 	
    	case string:
    		//Always update Accessed
    		

	    default:
	    	fmt.Println("In Default")
	    	break

	    }
    }
   return permission
}

func getPermissionFromInput(file_obj Params_struct) {



}
/** This function takes triplet(containing ky,rel,Value) from the structure and builds triplet with Value containing the above fields
*/
func buildValueJSONObject(file_obj Params_struct,permission string) string {

	val := file_obj.Value
	
	
	t := time.Now().In(time.UTC)
	
	var s string = t.Format("01/02/2006,15:04:05")

	// var p string= getPermission(file_obj)
		
	// size := strconv.Itoa(getSizeInBytes(val)) +"bytes"
	size := getSizeInBytes(val) +"bytes"

	
	var valueObj *ValueType_struct
	valueObj = &ValueType_struct{
								Content:val,
								Size:size,
								Created: s,
								Accessed: s,
								Modified: s,
								Permission: permission }		

	
	file_obj.Value = *valueObj
	b,_:=json.Marshal(file_obj)
	fmt.Println("Value JSON String is : ",string(b))

	return string(b)


}

//This function extracts contents of the value and returns Param Struct object
// with only three things key, rel, value (no timestamp, size, permission)
func extractContentIntoValue(file_obj Params_struct,permission *string) Params_struct{

	val := file_obj.Value

	var contentVal interface{}
	
    m1 := val.(map[string]interface{})
    // println("Successor is :")
    for k, v := range m1 {
    	 switch v.(type) {

		case interface{}:
			if k == "Content" {
				contentVal = v
			} else if k == "Permission" {
				*permission = v.(string)
			}

   
			break 	     	 	
    	case string:
    		//Always update Accessed
    		

	    default:
	    	fmt.Println("In Default")
	    	break

	    }
    }
    file_obj.Value = contentVal
    return file_obj



}

//This function updates input param structure object as per the fields passed to it
// field: If "Modified", modify the Modified field
//fieldContent : If "Content", modify content from the input "content" parameter
//*str_obj_record is the json string representation of the modified input object
func updateRecord(file_obj Params_struct,field string,fieldContent string,str_obj_record *string,content interface{},updated_permission string) Params_struct{

	fmt.Println(file_obj.Value)
	val := file_obj.Value
	
	
	t := time.Now().In(time.UTC)
	
	s := t.Format("01/02/2006,15:04:05")
	var accessed, modified, created,size,permission string
	var contentVal interface{}
	// var modValue interface{}
    m1 := val.(map[string]interface{})
    // println("Successor is :")
    for k, v := range m1 {
    	 switch v.(type) {

    	 
    	     	 	
		case interface{}:
			if k == "Content" {
				if fieldContent != "Content"{
				contentVal = v
				}else {
					contentVal = content
				}

			}else if k=="Accessed" {  //By Default modified	
    			println("\nAccessed date is ");print(v.(string))
    			


    			accessed = s
    			


    		} else if k=="Modified"{
    			if field != "Modified" {	
    				println("\nModified date is ");print(v.(string))
    				modified = v.(string)
    			}else {    //Modify only if Modifiled is specified in the field
    				modified = s
    			} 

    		}else if k=="Created"{	
    			println("\nModified date is ");print(v.(string))
    			created = v.(string)

    		}else if k=="Size"{	
    			if fieldContent == "Content"{
    				//Modify size
    				size = getSizeInBytes(content) +"bytes"
    				
    			}else{
    				size = v.(string)
    			}
    			
    		
    		} else if k == "Permission"{
    			if(updated_permission != "") {
    				permission = updated_permission
    			} else{
    				permission = v.(string)
    			}


    		}
   
			break 	     	 	
    	case string:
    		//Always update Accessed
    		

	    default:
	    	fmt.Println("In Default")
	    	break

	    }
    }

    var valueObj *ValueType_struct
	valueObj = &ValueType_struct{
								Content:contentVal,
								Size:size,
								Created: created,
								Accessed: accessed,
								Modified: modified,
								Permission: permission}	

	
	file_obj.Value = *valueObj
	b,_:= json.Marshal(file_obj)
	*str_obj_record = string(b)
	fmt.Println("Modified string is ",string(b))
	return file_obj								

} 

//It takes file_obj in basic form (i.e key, rel, value) and builds
//Json object with timestamp, permission etc and writes to persistent storage container
func write_to_file(file_obj Params_struct,permission string,reply *int) {

	*reply = 1;
	valueStr := buildValueJSONObject(file_obj,permission)
	// b, err := json.Marshal(file_obj)	
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

	if _, err = file.WriteString(valueStr+"\n"); err != nil {
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
				// if len(vv)==4{
				// 	input_obj.Permission=vv[3]
				// }
			}

	    default: //for ID
	    	
	    }
	}
	    return input_obj
}
//This function will search the file to see if triplet is preset matching given key and relation
//It sets flag = 1 if the record is found, else set it to zero
func search_in_file(filestring string,key string,rel string,flag *int,str_obj_record *string) error {

	file, err := os.Open(filestring)
	list_of_file_obj := make([]Params_struct,0)

    if err != nil {
       panic(err.Error())
    }

    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	
	// var str_obj string
	*flag = 0
     for scanner.Scan() {
		//unmarshal into Params_struct
		var file_obj Params_struct
		str_obj := scanner.Text()
		// fmt.Println("Json String is",*str_obj)
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil{
			if file_obj.Key == key && file_obj.Rel == rel {
				fmt.Println("\nFound the record")
				
				//Modify Accessed field
				var temp interface{} //No need for object modification, Pass empty content
				file_obj = updateRecord(file_obj,"Accessed","",str_obj_record,temp,"")
				*flag = 1
				// break	
			}
			
		} else {
			panic(err.Error())
		}
		list_of_file_obj = append(list_of_file_obj,file_obj)
		
		//fmt.Println(scanner.Text())
    }

    writeListOfObjectsToFile(list_of_file_obj)

	return nil
}



//This function will search the file for partial match
func partial_search_in_file(filestring string,key string,rel string,flag *int,str_obj *string,flag1 int) error {

	file, err := os.Open(filestring)

    if err != nil {
       panic(err.Error())
    }

	list_of_file_obj := make([]Params_struct,0)
    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    var partialreply string
    
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
			if  file_obj.Rel == rel && flag1==1{
				fmt.Println("\nFound a record")
				*flag = 1
				//Modify Accessed field
				var temp interface{} //No need for object modification, Pass empty content
				file_obj = updateRecord(file_obj,"Accessed","",str_obj,temp,"")
				partialreply+=*str_obj + ","
				
				
			}else if  file_obj.Key ==key && flag1==2{
				fmt.Println("\nFound a record")
				*flag = 1
				//Modify Accessed field
				var temp interface{} //No need for object modification, Pass empty content
				file_obj = updateRecord(file_obj,"Accessed","",str_obj,temp,"")
				partialreply+=*str_obj + ","
			}
			
		} else {
			panic(err.Error())
		}
		list_of_file_obj = append(list_of_file_obj,file_obj)
		//fmt.Println(scanner.Text())
    }

    writeListOfObjectsToFile(list_of_file_obj)
    //Remove last comma
    if(len(partialreply) >= 1){
    	//Slice
    	partialreply = partialreply[0:len(partialreply)-1]
    }
    *str_obj=partialreply
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
// func (t *Dict) Lookup(input_objPtr *Params_struct,reply *string) error {

// 	var key string;var rel string;var id int
// 	key = (*input_objPtr).Key
// 	rel = (*input_objPtr).Rel

// 	//Get hash value for data item
// 	hashValue := getHashValueForItem(key, rel)
// 	//Find the successor node
// 	succ_node := find_successor(hashValue)

// 	fmt.Println("Hash value of the data :",hashValue)
// 	//Check if current node is successor node. If not then call RPC to insert at successor node

// 	if (succ_node.Id != config_obj.ServerID){
// 		fmt.Println("In IFFFF")
// 		// succ_node := find_successor(hashValue)
// 		fmt.Println("Successor node is ",succ_node.Port)
// 		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
// 	      defer c.Close()
// 		  var reply1 string

// 		 rpc_call := c.Go("Dict.Lookup",input_objPtr,&reply1,nil)		
// 		  <-rpc_call.Done

// 		  fmt.Println("If conf reply is : ",reply1)
// 		  *reply=reply1
// 		 // println("output is : ");println(output.IpAddress);println(output.Port)
// 		 if err != nil {
// 		 	log.Fatal("Dict error:",err);
// 		 }		

// 	} else { 


// 				var str_obj string
// 			flag := 0	
// 			search_in_file(filename,key,rel,&flag,&str_obj)
// 			var resp_obj *Response_message
// 			if flag == 1 {
				
// 				//Construct a reply message
// 				resp_obj = &Response_message{
// 								Result:str_obj,
// 								Id: id,
// 								Error: "null"}		
// 			} else {
// 				//Construct a reply message
// 				fmt.Println("\nNo matching record found")
// 				resp_obj = &Response_message{
// 								Result:"null",
// 								Id: id,
// 								Error: "null"}		
// 			}
// 			b,_ := json.Marshal(resp_obj)
// 			*reply = string(b)  //Set the reply
// 	}
// 	// input_obj := extract_params(f,&id)
// 	// key = input_obj.Key
// 	// rel = input_obj.Rel
	
// 	fmt.Println("Reply sent is ",*reply)
// 	return nil
	
// }


func (t *Dict) LookupOnRel(input_objPtr *Params_struct,reply *string) error {

	var key string;var rel string
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel

	if(rel != "" && key == ""){


			var str_obj string
			flag := 0	
			partial_search_in_file(filename,key,rel,&flag,&str_obj,1) //last parameter 1 for match on Rel
			// var resp_obj *Response_message
			if flag == 1 {
				
				*reply = str_obj
			} else {
				//Construct a reply message
				fmt.Println("\nNo matching record found")
				*reply = str_obj
			}

	}

	return nil

}


func (t *Dict) LookupOnVal(input_objPtr *Params_struct,reply *string) error {

	var key string;var rel string
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel

	if(rel == "" && key != ""){


			var str_obj string
			flag := 0	
			partial_search_in_file(filename,key,rel,&flag,&str_obj,2) //last parameter 1 for match on Rel
			// var resp_obj *Response_message
			if flag == 1 {
				
				*reply = str_obj
			} else {
				//Construct a reply message
				fmt.Println("\nNo matching record found")
				*reply = str_obj
			}

	}

	return nil

}


//Lookup that handles partial match
func (t *Dict) Lookup(input_objPtr *Params_struct,reply *string) error {
	
	// var f interface{}
	// //Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
	// 	log.Fatal("error:",err);
	// }
	var key string;var rel string
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	
	if(key!="" && rel!=""){

	hashValue := getHashValueForItem(key, rel)
	//Find the successor node
	succ_node := find_successor(hashValue)

	fmt.Println("Hash value of the data :",hashValue)
	//Check if current node is successor node. If not then call RPC to insert at successor node

	if (succ_node.Id != config_obj.ServerID){
		fmt.Println("In IFFFF")
		// succ_node := find_successor(hashValue)
		fmt.Printf("Successor node port : %d   ID : %d",succ_node.Port,succ_node.Id)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
		defer c.Close()
	      var reply1 string

		 err_call := c.Call("Dict.Lookup",input_objPtr,&reply1)		
	      if(err_call != nil){

	      		fmt.Println("Error while calling RPC in partial match. Callling to node ",succ_node.Id)
	      }

		 // rpc_call := c.Go("Dict.Lookup",input_objPtr,&reply1,nil)		
		 //  <-rpc_call.Done

		  fmt.Println("If conf reply is : ",reply1)
		  *reply=removebackslash(reply1)
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
									Error: "null"}		
				} else {
					//Construct a reply message
					fmt.Println("\nNo matching record found")
					resp_obj = &Response_message{
									Result:"null",
									Error: "null"}		
				}
				b,_ := json.Marshal(resp_obj)
				*reply = removebackslash(string(b))  //Set the reply
		}
		// input_obj := extract_params(f,&id)
		// key = input_obj.Key
		// rel = input_obj.Rel
		
		fmt.Println("Reply sent is ",*reply)



}else if(key==""){
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		visitednodes := make([]int,0)
		var valueToSearch int
		hashValue := getHashValueForItem(key, rel)
		
		a:= hashValue & 15
		listOfPartialMatchArray := make([]string,0)
		var replyPartialMatch string = ""
		for i:=0;i<16;i++ {
			
			c:= i << 4
			b:=a | c
			valueToSearch=b
			succ_node := find_successor(valueToSearch)
			visited:=0

			for _,visit:=range visitednodes{
				if (visit==succ_node.Id){
					visited=1
				}
			}
			//Already visited this node, so do not make an RPC.
			if (visited==1){
				continue;

			}

			//Make an RPC to the successor
			fmt.Println("Hash value of the data :",valueToSearch)
			fmt.Println("Making RPC for partial match to ",succ_node.Id)

			c1ient, _ := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
			defer c1ient.Close()
		    var reply1 string

	      	rpc_call := c1ient.Go("Dict.LookupOnRel",input_objPtr,&reply1,nil)		
		  	<-rpc_call.Done
		  	replyPartialMatch += reply1
		  	if(len(reply1) >= 1) {  //Do not add empty string to results
		  		listOfPartialMatchArray = append(listOfPartialMatchArray,reply1)
		  	}

			//Add node to visited list
			visitednodes=append(visitednodes,succ_node.Id)
		}//for loop ends
		//Construct reply and send
		//Marshal the array
		resultString,_ := json.Marshal(listOfPartialMatchArray)

		var resp_obj *Response_message
		resp_obj = &Response_message{
						Result:string(resultString),
						Error: "null"}		
		
		b,_ := json.Marshal(resp_obj)
		*reply = removebackslash(string(b) )
	//Partial match for rel ends			
	
	} else if(rel==""){

		visitednodes := make([]int,0)
		var valueToSearch int
		hashValue := getHashValueForItem(key, rel)
		
		a:= hashValue & 240
		listOfPartialMatchArray := make([]string,0)
		var replyPartialMatch string = ""
		for i:=0;i<16;i++ {
			
			b:=a | i
			
			valueToSearch=b
			succ_node := find_successor(valueToSearch)
			visited:=0

			for _,visit:=range visitednodes{
				if (visit==succ_node.Id){
					visited=1
				}
			}
			//Already visited this node, so do not make an RPC.
			if (visited==1){
				continue;

			}

			//Make an RPC to the successor
			fmt.Println("Hash value of the data :",valueToSearch)
			fmt.Println("Making RPC for partial match to ",succ_node.Id)

			c1ient, _ := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
			defer c1ient.Close()
		    var reply1 string

	      	rpc_call := c1ient.Go("Dict.LookupOnVal",input_objPtr,&reply1,nil)		
		  	<-rpc_call.Done
		  	replyPartialMatch += reply1
		  	if(len(reply1) >= 1) {  //Do not add empty string to results
		  		listOfPartialMatchArray = append(listOfPartialMatchArray,reply1)
		  	}
		  	

			//Add node to visited list
			visitednodes=append(visitednodes,succ_node.Id)
		}//for loop ends
		//Construct reply and send
		//Marshal the array
		resultString,_ := json.Marshal(listOfPartialMatchArray)

		var resp_obj *Response_message
		resp_obj = &Response_message{
						Result:string(resultString),
						Error: "null"}		
		
		b,_ := json.Marshal(resp_obj)
		*reply = removebackslash(string(b) )
	//Partial match for key ends		
	}

	return nil
	
}

//Insert the triplet if does not already exists and return true, else return false
func (t *Dict) Insert(input_objPtr1 *Params_struct_with_perm,reply *string) error {

	// var f interface{}
	var write_file_reply int
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
		// log.Fatal("error:",err);
	// }
	var permission = (*input_objPtr1).Permission
	var key string;var rel string
	key = (*input_objPtr1).Key
	rel = (*input_objPtr1).Rel

	var input_objPtr *Params_struct
	input_objPtr = &Params_struct{
									Key:key,
									Rel: rel,
									Value:(*input_objPtr1).Value}

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
	//	succ_node := find_successor(hashValue)
		fmt.Printf("Successor node port : %d   ID : %d",succ_node.Port,succ_node.Id)
		c, err := jsonrpc.Dial(config_obj.Protocol, succ_node.IpAddress +":"+strconv.Itoa(succ_node.Port))
	      defer c.Close()
		  var reply1 string

		 rpc_call := c.Go("Dict.Insert",input_objPtr1,&reply1,nil)		
		  <-rpc_call.Done

		  fmt.Println("If conf reply is : ",reply1)
		  *reply=removebackslash(reply1)
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
			fmt.Println("Record already exists")
			resp_obj = &Response_message{
								Result:"false",
								Error: "Record already exists"}		
		} else {
			write_to_file(*input_objPtr,permission,&write_file_reply)
			if write_file_reply == 0 {
			
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"true",
								Error: "null"}		
			} else {
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"false",
								Error: "Error writing to file"}		
			}	
		}

		b,_:= json.Marshal(resp_obj)
		*reply = removebackslash(string(b))  //Set the reply
	}
	fmt.Println("Reply sent is : ",*reply)
	// println("Done with the call..\n")
	return nil

}


//This is to get data from predecessor when it is about to leave
func (t *Dict) InsertOnShutdown(input_objPtr *Params_struct,reply *string) error {

	// var f interface{}
	var write_file_reply int
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
		// log.Fatal("error:",err);
	// }
	
	var key string;var rel string
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	// input_obj := extract_params(f,&id)
	// key = input_obj.Key
	// rel = input_obj.Rel
	
	
	//Check if current node is successor node. If not then call RPC to insert at successor node
	  //Successor is current node, insert here
	//	fmt.Println("In else")
		var str_obj string
		flag := 0	

		search_in_file(filename,key,rel,&flag,&str_obj)
		var resp_obj *Response_message
		if flag == 1 {
			fmt.Println("Record already exists")
			resp_obj = &Response_message{
								Result:"false",
								Error: "Record already exists"}		
		} else {
			//Extract Content and store in it Value
			var permission string
			*input_objPtr = extractContentIntoValue(*input_objPtr,&permission)
			write_to_file(*input_objPtr,permission,&write_file_reply)
			if write_file_reply == 0 {
			
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"true",
								Error: "null"}		
			} else {
				//Construct a reply message
				resp_obj = &Response_message{
								Result:"false",
								Error: "Error writing to file"}		
			}	
	}

		b,_:= json.Marshal(resp_obj)
		*reply = removebackslash(string(b))  //Set the reply
	
	fmt.Println("Reply sent is : ",*reply)
	// println("Done with the call..\n")
	return nil

}

//Read triplets from file, store them in list of Param_struct object.
//If any triplet matches with given key and relation, then update its value with new value
//In the end, write entire list back to the file
func (t* Dict) InsertOrUpdate(input_objPtr1 *Params_struct_with_perm,reply *string) error{
	
	
	


	var permission = (*input_objPtr1).Permission
	var key string;var rel string
	key = (*input_objPtr1).Key
	rel = (*input_objPtr1).Rel

	var input_objPtr *Params_struct
	input_objPtr = &Params_struct{
									Key:key,
									Rel: rel,
									Value:(*input_objPtr1).Value}	

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
		defer c.Close()
		 rpc_call := c.Go("Dict.InsertOrUpdate",input_objPtr1,&reply1,nil)		
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
			
			var str_obj string
			flag := 0
			list_of_file_obj := make([]Params_struct,0)

		    for scanner.Scan() {
				//unmarshal into Params_struct
				var file_obj Params_struct
				str_obj = scanner.Text()
				err = json.Unmarshal([]byte(str_obj),&file_obj)









				if err == nil{
					  
					if file_obj.Key == key && file_obj.Rel == rel {
						
						if(getPermission(file_obj)=="RW" ){
							fmt.Println("\nFound the record")
							//Content to be modified, pass new content
							file_obj = updateRecord(file_obj,"Modified","Content",&str_obj,(*input_objPtr).Value,permission)
							
							fmt.Println(file_obj.Value)
						}
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
		    	write_to_file((*input_objPtr),permission,&write_file_reply)
		    	if write_file_reply == 0 {
		    		fmt.Println("Record does not exist..Inserting")
		    	} else {
		    		fmt.Println("Error while inserting")
		    	}
		    	
		    } else {
		   		fmt.Println("Updating the record")
				writeListOfObjectsToFile(list_of_file_obj)
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
	
	
	var key string;var rel string;
	key = (*input_objPtr).Key
	rel = (*input_objPtr).Rel
	
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
		defer c.Close()
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

					if file_obj.Key == key && file_obj.Rel == rel && getPermission(file_obj)=="RW" {
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
	// extract_params(f,&id)
	file, err := os.Open(filename)

    if err != nil {
       panic(err.Error())
     }
    
    list_of_file_obj := make([]Params_struct,0)
    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	
	var str_obj string

    for scanner.Scan() {
    	var file_obj Params_struct
		//unmarshal into Params_struct
		str_obj = scanner.Text()
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil {
			//Add to map
			key_rel_map[file_obj.Key] = file_obj.Rel
			//Modify Accessed field
			var temp interface{} //No need for object modification, Pass empty content
			file_obj = updateRecord(file_obj,"Accessed","",&str_obj,temp,"")

			
		} else {
			panic(err.Error())
		}
		list_of_file_obj = append(list_of_file_obj,file_obj)
		//fmt.Println(scanner.Text())
    }
    file.Close()

    //Modify Accessed
    writeListOfObjectsToFile(list_of_file_obj)

    list_of_keys := make([]string,0)
    for k, _ := range key_rel_map {
    	list_of_keys = append(list_of_keys,k)
    }
    //Marshal the array to convert to string
    string_arr,_ := json.Marshal(list_of_keys)
    println("Reply from function is ",string(string_arr))
    return string(string_arr)



    
}

func constructListReply(list_of_keys_string string)string{

	if(len(list_of_keys_string) > 0){
		list_of_keys_string = list_of_keys_string[0:len(list_of_keys_string)-1]
	}
	resp_obj := &Response_message{
						Result:"["+list_of_keys_string+"]",
						Error: "null"}	

    b,_ := json.Marshal(resp_obj)
    
    

	// println("Done with the call..\n")
	return string(b)

}

//Read triplets from the file, store key, relation pair into the map and return the list of keys
func (t *Dict) ListKeys(input_objPtr *Params_struct, reply *string) error {
	
	
	// fmt.Println("Reply string before calling the function is : ",*reply)
	

	var key string;
	key = (*input_objPtr).Key
	// println("Key is : ",key)
	// hashValue := getHashValueForItem(config_obj.IpAddress, strconv.Itoa( config_obj.Port))

	if(len(key) == 0){ //First node initiating request insert its own ID in key and make RPC to Successor

		fmt.Println("First node initiating listKeys ")	

		list_of_keys_reply := getListOfKeys()
		list_of_keys_reply =convertToResult(list_of_keys_reply)
		// fmt.Println("After Conversion ",list_of_keys_reply)
		// fmt.Printf("Length After Conversion %d",len(list_of_keys_reply))
				
		(*input_objPtr).Key = strconv.Itoa(config_obj.ServerID)

		//

		

		if(successor.Id != config_obj.ServerID) { //node is successor of itself, one node case
			fmt.Println("Successor node is ",successor.Port)

			c, _ := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
		       var reply1 string
		     defer c.Close()

			 rpc_call := c.Go("Dict.ListKeys",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
			  if(len(list_of_keys_reply) == 0){

			 		( *reply) += list_of_keys_reply +reply1
			  }else {
			  		( *reply) += list_of_keys_reply +","+reply1
			  }
			
			 *reply = removebackslash(constructListReply(*reply))
			 //TO-DO This is final reply to be sent. Construct reply here
			  // fmt.Println("Reply string after RPC to next node is : ",*reply)

		} else {

			*reply = list_of_keys_reply
			*reply = removebackslash(constructListReply(*reply))
			//TO-DO This is final reply to be sent. Construct reply here
			
		}

		

	} else {

		id, err := strconv.Atoi(key)
		
		
		if err != nil {
    		panic(err)	
		}
		if(id != config_obj.ServerID){ //Checking if it is the first node
			//make an RPC call
			
			list_of_keys_reply := getListOfKeys()
		list_of_keys_reply =convertToResult(list_of_keys_reply)


			//PRC to sucessor
			
			fmt.Println("Successor node is ",successor.Port)
			c, _ := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
		       var reply1 string
		  defer c.Close()

			 rpc_call := c.Go("Dict.ListKeys",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
			
			 if(len(list_of_keys_reply) == 0){

			 		( *reply) += list_of_keys_reply +reply1
			  }else {
			  		( *reply) += list_of_keys_reply +","+reply1
			  }
			
			  // fmt.Println("Reply string after RPC to next node is : ",*reply)


		}else{
			(*reply)=""
			// *reply = constructListReply(*reply)
		} 


	}
	
	return nil
	
}

func getListOfIDs() string{

	key_rel_map := make(map[string]string)
	// var f interface{}
	//Unmarshal in map of string to interface
	// err := json.Unmarshal([]byte(*args), &f)
	// if err != nil {
		// log.Fatal("error:",err);
	// }
	
	// extract_params(f,&id)
	file, err := os.Open(filename)

    if err != nil {
       panic(err.Error())
     }
    list_of_file_obj := make([]Params_struct,0)

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	
	var str_obj string
	//two dimensional array tp hold key relation pair
	list_of_keys := make([][]string,0)
    for scanner.Scan() {
    	var file_obj Params_struct
		//unmarshal into Params_struct
		str_obj = scanner.Text()
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil {
			//Add to map
			key_rel_map[file_obj.Key] = file_obj.Rel
			//Modify Accessed field
			var temp interface{} //No need for object modification, Pass empty content
			file_obj = updateRecord(file_obj,"Accessed","",&str_obj,temp,"")
			key_rel_pair := []string{file_obj.Key,file_obj.Rel}
    	
    		list_of_keys = append(list_of_keys,key_rel_pair)
			
		} else {
			panic(err.Error())
		}
		list_of_file_obj = append(list_of_file_obj,file_obj)
		//fmt.Println(scanner.Text())
    }
    file.Close()


    //Write to the file
    writeListOfObjectsToFile(list_of_file_obj)


    //Marshal the array to convert to string
    string_arr,_ := json.Marshal(list_of_keys)
    // resp_obj := &Response_message{
				// 		Result:string(string_arr),
				// 		Id: id,
				// 		Error: "null"}	

    // b,_ := json.Marshal(resp_obj)
    
    return string(string_arr)
    
	
}

func writeListOfObjectsToFile(list_of_file_obj []Params_struct){

				fileptr, _ := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC,0600)
			for _, v := range list_of_file_obj {
					b, err := json.Marshal(v)
					if _, err = fileptr.WriteString(string(b)+"\n"); err != nil {
				panic(err)
		 		}

			}
			fileptr.Close() 

}
func convertToResult(str string) string{
	var y string
	if(len(str) >=1 ){
		y = str[0:len(str)-1]
	}
	if (len(y) >1 ){
		y = y[1:len(y)]
		return y
	}
	return ""
	


}

//This function returns list of key,relation pair in paramater *reply
func (t *Dict) ListIDs(input_objPtr *Params_struct, reply *string) error {
	
	// fmt.Println("Reply string before calling the function is : ",*reply)
	

	var key string;
	key = (*input_objPtr).Key
	// println("Key is : ",key)
	// hashValue := getHashValueForItem(config_obj.IpAddress, strconv.Itoa( config_obj.Port))

	if(len(key) == 0){ //First node initiating request insert its own ID in key and make RPC to Successor

		fmt.Println("First node initiating listKeys ")	

		list_of_ids_reply := getListOfIDs()
		fmt.Println("Get ListIDs Reply : ",list_of_ids_reply)
		list_of_ids_reply = convertToResult(list_of_ids_reply)
		(*input_objPtr).Key = strconv.Itoa(config_obj.ServerID)
		//PRC to sucessor
		// succ_node := find_successor(hashValue)
		fmt.Println("Get ListIDs Successor node is ",successor.Port)
		if(successor.Id != config_obj.ServerID) { //node is successor of itself, one node case
			

			c, _ := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
		       var reply1 string
		     defer c.Close()

			 rpc_call := c.Go("Dict.ListIDs",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
			 
			 	 if(len(list_of_ids_reply) == 0){

			 		( *reply) += list_of_ids_reply +reply1
			  }else {
			  		( *reply) += list_of_ids_reply +","+reply1
			  }
			 //TO-DO This is final reply to be sent. Construct reply here
			 *reply = removebackslash( constructListReply(*reply))
			  // fmt.Println("Reply string after RPC to next node is : ",*reply)
		} else {

			*reply = list_of_ids_reply
			*reply =removebackslash(constructListReply(*reply))
			//TO-DO This is final reply to be sent. Construct reply here
		}

		

	} else {
		id, err := strconv.Atoi(key)
		
		
		if err != nil {
    		panic(err)	
		}
		if(id != config_obj.ServerID){ //Checking if it is the first node
			//make an RPC call
			
			list_of_ids_reply := getListOfIDs()
			fmt.Println("Get ListIDs Reply : ",list_of_ids_reply)
			list_of_ids_reply = convertToResult(list_of_ids_reply)


			//PRC to sucessor
			// succ_node := find_successor(hashValue)
			fmt.Println("Successor node is ",successor.Port)
			c, _ := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
		       var reply1 string
		  defer c.Close()

			 rpc_call := c.Go("Dict.ListIDs",input_objPtr,&reply1,nil)		
			  <-rpc_call.Done
			  
			 
			 	 if(len(list_of_ids_reply) == 0){

			 		( *reply) += list_of_ids_reply +reply1
			  }else {
			  		( *reply) += list_of_ids_reply +","+reply1
			  }
				
		
			  


		}else{
			(*reply)=""
			
		} 


	}
	
	return nil
}
// //This function closes the existing connection, stops the listener and then exit the server program
// func (t *Dict) Shutdown(input_objPtr *Params_struct,reply *string) error {
	
// 	fmt.Println("\nClosing the server!!")
// 	*reply = "shutdown successful!!"
// 	conn.Close()
// 	listener.Close()
// 	os.Exit(0)
// 	return nil
// }

func (t* Dict) NotifyToPredecessorOnShutDown(succ_node *Nodeid,reply *string) error{
successor=*succ_node

return nil
}

func (t* Dict) NotifyToSuccessorOnShutDown(pred_node *Nodeid,reply *string) error{
predecessor = *pred_node

return nil
}
func (t *Dict) Shutdown(input_objPtr *Params_struct,reply *string) error {
	

 var str_obj string
			// flag := 0	
			// search_in_file(filename,key,rel,&flag,&str_obj)
			

file, err := os.Open(filename)

    if err != nil {
       panic(err.Error())
    }

    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)

    scanner.Split(bufio.ScanLines)
	 var file_obj Params_struct
	// var str_obj string
	// *flag = 0
     for scanner.Scan() {
		//unmarshal into Params_struct
		str_obj = scanner.Text()
		// fmt.Println("Json String is",*str_obj)
		err = json.Unmarshal([]byte(str_obj),&file_obj)
		if err == nil{
			//insert into successor node
			fmt.Println("Passing data to the Successor",successor.Port)
			c, err1 := jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
			if err1 != nil {
		 			log.Fatal("Dict error:",err);
						 }		
			defer c.Close()
	      var reply1 string

		 rpc_call := c.Go("Dict.InsertOnShutdown",&file_obj,&reply1,nil)		
		  <-rpc_call.Done

		 
		 //Notifying my predecessor of my successor, so it will change its it successor to my successor
		  	c, err = jsonrpc.Dial(config_obj.Protocol, predecessor.IpAddress +":"+strconv.Itoa(predecessor.Port))
			if err != nil {
		 			log.Fatal("Dict error:",err);
						 }		
			defer c.Close()
		 rpc_call = c.Go("Dict.NotifyToPredecessorOnShutDown",&successor,&reply1,nil)		
		  <-rpc_call.Done

//Notifying my successor of my predecessor, so it will change its it predecessor to my predecessor
		  c, err = jsonrpc.Dial(config_obj.Protocol, successor.IpAddress +":"+strconv.Itoa(successor.Port))
			if err != nil {
		 			log.Fatal("Dict error:",err);
						 }		
			defer c.Close()
		 rpc_call = c.Go("Dict.NotifyToSuccessorOnShutDown",&predecessor,&reply1,nil)		
		  <-rpc_call.Done


		 		
		}else {
			panic(err.Error())
		}	
		 
		
		//fmt.Println(scanner.Text())
    }



	fmt.Println("\nClosing the server!!")
	*reply = "shutdown successful!!"
	conn.Close()
	listener.Close()
	os.Exit(0)
	return nil
}




func Purge(){


file, err := os.Open(filename)
	list_of_file_obj := make([]Params_struct,0)

    if err != nil {
       panic(err.Error())
    }

    defer file.Close()

    reader := bufio.NewReader(file)
    scanner := bufio.NewScanner(reader)
	
    scanner.Split(bufio.ScanLines)
	

	// var str_obj string
	
     for scanner.Scan() {
		//unmarshal into Params_struct
		var file_obj Params_struct
		str_obj := scanner.Text()
		// fmt.Println("Json String is",*str_obj)
		err = json.Unmarshal([]byte(str_obj),&file_obj)
//		var thisVal ValueType_struct
		//thisVal=file_obj.Value.(ValueType_struct)

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	val := file_obj.Value
	
	
//	t := time.Now()
	
//	s := t.Format("01/02/2006,15:04:05")
	var accessed string
	//var contentVal interface{}
	// var modValue interface{}
    m1 := val.(map[string]interface{})

    // println("Successor is :")
    for k, v := range m1 {
    	 switch v.(type) {

    	 
    	     	 	
		case interface{}:
			// if k == "Content" {
			// 	if fieldContent != "Content"{
			// 	contentVal = v
			// 	}else {
			// 		contentVal = content
			// 	}

		 if k=="Accessed" {  //By Default modified	
    	//		println("\nAccessed date is ");print(v.(string))
    			


    			accessed = v.(string)
    			


    		} 
    		

	    default:
	    	fmt.Println("In Default")
	    	break

	    }
    }



/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// var UTC *Location = &utcLoc		
		t := time.Now().In(time.UTC)
		t1,_:=time.Parse("01/02/2006,15:04:05 ",accessed)
		// t,_:=time.Parse("01/02/2006,15:04:05",tstring)
	//var s string = t.Format("01/02/2006,15:04:05")
	//	if err == nil{
			diff :=  t.Sub(t1)
			fmt.Println("\n\n\n\n\n\n\n\n\n\n  ")
			fmt.Println("Difference in time  : ",diff )
			 fmt.Println(" time to live: ", Time_to_live , " t:",t, "t1:", t1)
			if  diff < Time_to_live || getPermission(file_obj) =="R" {
								
				
				list_of_file_obj=append(list_of_file_obj,file_obj)

			}
			
	//	} else {
	//		panic(err.Error())
	//	}
		
		
	
    }
  
    writeListOfObjectsToFile(list_of_file_obj)
	fmt.Println("\nDeleted records..")
	
//time.Sleep(60* time.Second* 2 )  /////substitute time_to_live
    timeVar := time.Now().In(time.UTC)
	fmt.Println("---This is Purge---",timeVar.Format("20060102150405"))


}



var listener net.Listener //this holds the Listener object
var conn net.Conn //This holds the connection
var dict *Dict
var Time_to_live time.Duration

func startServer() {
	
    dict = new(Dict)
  
    server := rpc.NewServer()
    server.Register(dict)
     
	
    //Call find successor
   	// fix_my_successor()
    var e error;var err error
    listener, e = net.Listen(config_obj.Protocol, ":"+strconv.Itoa(config_obj.Port))

    if e != nil {
        log.Fatal("listener error:", e)
        
    }
    println("\nAccepting connections...\n")
    
	    
      if (config_obj.IsFirstNode !=1) {
         Join();
    	}

    for {
		    
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
	runtime.GOMAXPROCS(8)
	var wg sync.WaitGroup

if len(os.Args) != 2{
 		fmt.Println("Specify server configuration file")
 		return
 	}

	read_server_config_file(os.Args[1])



    wg.Add(4)
    go func() {
        defer wg.Done()
    startServer()

    }()

    //var input string
	//fmt.Scanln(&input)
	//fmt.Println("Done")    

    go func() {
      defer wg.Done()
	  println("\nfinger thread executing")        
      time.Sleep(1000* time.Microsecond)
      fix_fingers()
    }()

    go func() {
    	defer wg.Done()
    	for{
    	
    	println("\nStabilize executing")        
      time.Sleep(1000* time.Microsecond)
      Stabilize()
  			}
    	}()

    go func() {
    	
    	defer wg.Done()
    	for{
    	
    	println("\nCheckPredecessor executing")        
      time.Sleep(1000* time.Microsecond)
      CheckPredecessor()
  			}
    	}()

    	go func() {
    	defer wg.Done()
    	for{
    	
    	println("\nFix fingers executing")        
      time.Sleep(1000* time.Microsecond)
      fix_fingers()
  			}
    	}()



     go func() {
    	defer wg.Done()
    	for{
    	
    	println("\nPurge executing")        
    	
     // Time_to_live=TimetoLive*time.Minute
      Purge()
       time.Sleep(time.Minute * 2)
  			}
    	}()

    wg.Wait()

	

}
