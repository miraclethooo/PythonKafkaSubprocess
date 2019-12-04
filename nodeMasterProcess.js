const childProcess = require('child_process');                                           
const children = [];    

function createPythonChild() {
  const spawn = childProcess.spawn;
  const child = spawn('python', ['pythonKafkaConsumer.py'], { stdio: [ 'pipe', 'pipe', 2 ] });
  child.stdout.on('data', (buffer) => {
    let stringBuffer = buffer.toString();

    if (child.lastLine) { 
      stringBuffer = child.lastLine + stringBuffer                                       
      child.lastLine = undefined;
    }

    let startOfSplit = 0; 
    const splits = [];  

    let i=0;
    if (stringBuffer.length > 3) {
      for (i; i<stringBuffer.length - 2; i++) {                                          
        // Messages from the python process are delimited by chr(3) + chr(0) + chr(4)    
        if (stringBuffer.charCodeAt(i) === 3 &&
            stringBuffer.charCodeAt(i+1) === 0 &&
            stringBuffer.charCodeAt(i+2) === 4) {
          splits.push(stringBuffer.substring(startOfSplit, i))
          startOfSplit = i+3;                                                            
        }                                                                                
      }
    }

    if (startOfSplit < i) {
      child.lastLine = stringBuffer.substring(startOfSplit, stringBuffer.length);        
    }  

    for (let j=0; j<splits.length; j++) {
      console.log(split);                                                                                                                                                         
    }       
  });     

  children.push(child);

  return child;                                                                          
}
function childReadTopic(child, topicName) {                                              
  child.stdin.write('{"command": "readTopic", "topic": "' + topicName+ '"}\n');          
}

const child = createPythonChild();
childReadTopic(child, 'yabadabadoo---welcome_members_of_yabadabadoo---2019-11-26');      
