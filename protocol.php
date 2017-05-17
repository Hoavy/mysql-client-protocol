<?php
error_reporting(E_ALL);
define('STATE_CONNECTED', 1);
define('STATE_COMMAND_SENT', 2);
define('COM_QUERY', 0x03);
define('SERVER_MORE_RESULTS_EXISTS', 8);


class MySQLClient{
    var $socket = NULL;
    var $_max_packet_size = 0;
    var $database;
    var $user;
    var $host;
    var $port;
    var $state;
    var $protocol_ver;
    var $_server_ver;
    
    var $_server_capabilities;
    var $packet_no; 
    
    var $compact;
    
    var $converters = array();
    
    
    public function My(){
        $this->converters[0x01] = 1;
        $this->converters[0x02] = 1;
        $this->converters[0x03] = 1;
        $this->converters[0x04] = 1;
        $this->converters[0x05] = 1;
        //$this->converters[0x09] = 1;
        //$this->converters[0x0d] = 1;
        //$this->converters[0xf6] = 1;
    }
    
    
    
    public function _dump($data){
        $byte  = array();
        for($i = 0; $i < strlen($data); $i++) {
            $byte[$i] = ord($data[$i]);
        }
        return implode(" ", $byte);
    }
    
    public function _get_byte2($data, $i){      
        list($a, $b) = array($this->strbyte($data, $i), $this->strbyte($data, $i+1));
        $bor = $a | ($b << 8);
        $len = $i + 2;
        return array($bor,$len);
    }
    
    public function _get_byte3($data, $i){
        list($a, $b, $c) = array($this->strbyte($data, $i), $this->strbyte($data, $i+1),$this->strbyte($data, $i+2));
        $bor  = $a | ($b << 8)  | ($c << 16);
        $len = $i + 3;
        return array($bor,$len);
    }
    
    public function _get_byte4($data, $i){
        list($a, $b, $c, $d) = array($this->strbyte($data, $i), $this->strbyte($data, $i+1),$this->strbyte($data, $i+2),$this->strbyte($data, $i+3));
        $bor  = $a | ($b << 8)  | ($c << 16) | ($d << 24);
        $len = $i + 4;
        return array($bor,$len);
    }
    
    public function _get_byte8($data, $i){
        list($a, $b, $c, $d,$e,$f,$g,$h) = array($this->strbyte($data, $i), $this->strbyte($data, $i+1),$this->strbyte($data, $i+2),$this->strbyte($data, $i+3),$this->strbyte($data, $i+4),$this->strbyte($data, $i+5),$this->strbyte($data, $i+6),$this->strbyte($data, $i+7));
        $lo = $a | ($b << 8) | ($c << 16) | ($d << 24);
        $hi = $e | ($f << 8) | ($g << 16) | ($h << 24);
        return array($lo + $hi * 4294967296, $i + 8);
    }
    
    public function _set_byte2($n){
        return chr($this->_band($n,0xff)).chr($this->_band($this->_rshift($n, 8),0xff));
    }
    
    public function _set_byte3($n){
        return chr($this->_band($n,0xff)).chr($this->_band($this->_rshift($n, 8),0xff)).chr($this->_band($this->_rshift($n, 16),0xff));
    }
    
    public function _set_byte4($n){
        return chr($this->_band($n,0xff)).chr($this->_band($this->_rshift($n, 8),0xff)).chr($this->_band($this->_rshift($n, 16),0xff)).chr($this->_band($this->_rshift($n, 24),0xff));
    }
    
    
    public function strbyte($data, $pos = 1){
        if(strlen($data) < $pos){
            return NULL;
        }
        return ord($data[$pos-1]);
    }
    
    public function _band($n, $f){
        return $n & $f;
    }
    
    public function _rshift($n, $pos){
        return $n >> $pos;
    }
    
    
    public function _to_cstring($data){
        return array($data, "\0");
    }
    
    
    public function _to_binary_coded_string($data){
        return array(chr(strlen($data)),$data);
    }
    
    
    public function strrep($seed,$count){
        $byte  = array();
        for($i = 0; $i < $count; $i++) {
            $byte[$i] = $seed;
        }       
        return implode("", $byte);
    }
    
    public function _from_length_coded_str($data, $pos){
        $len = 0;
        list($len, $pos) = $this->_from_length_coded_bin($data, $pos);
        if($len == NULL){
            return array(null, $pos);
        }
        return array($this->_substr($data, $pos, $pos + $len - 1),$pos + $len);
    }   
    
    
    
    
    
    public function _recv_packet(){
        $data = socket_read($this->socket, 4);
        
        list($len, $pos) = $this->_get_byte3($data, 1);
        
        $_max_packet_size = 1024 * 1024;
        if($len > $_max_packet_size){
            printf("<p>packet size too big : %d </p>", $_max_packet_size);
        }

        $num = $this->strbyte($data, $pos);
        
        $this->packet_no = $num;
        
        $data = socket_read($this->socket,$len);
        
        if(empty($data)){
            echo "<p>failed to read packet content.</p>";
        }
        
        $field_count = $this->strbyte($data, 1);
        
        $typ = "";
        if($field_count == 0x00 ){
            $typ = "OK";
        }elseif ($field_count == 0xff){
            $typ = "ERR";
        }elseif($field_count == 0xfe){
            $typ = "EOF";
        }elseif($field_count <= 250 ){
            $typ = "DATA";
        }
        return array($data,$typ);
    }
    
    public function _send_packet($req, $size){
        $this->packet_no = $this->packet_no + 1;
        $packet = $this->_set_byte3($size).
            chr($this->packet_no).
            $req;
        return socket_write($this->socket, $packet, strlen($packet));
    }
    
    
    
    public function _parse_err_packet($packet){
        list($errno, $pos) = $this->_get_byte2($packet, 2);
        $marker = $this->_substr($packet, $pos, $pos);
        $sqlstate = "";
        if($marker == "#"){
            $pos = $pos + 1;
            $sqlstate = $this->_substr($packet, $pos, $pos + 5 - 1);
            $pos = $pos + 5;
        }
        
        $message = $this->_substr($packet,$pos);
        return array($errno, $message, $sqlstate);
    }   
    
    public function _from_cstring($data, $i = 0){
        $last = stripos($data, "\0", $i);
        if($last < 1){
            return array(false, false);;
        }
        $subs = substr($data, $i-1, $last);
        $last = $last + 2;
        return array($subs, $last);
    }
    
    
    public function _substr($packet, $pos, $end = NULL){
        if($end == NULL){
            return substr($packet, $pos-1); 
        }
        $len = $end - $pos;
        return substr($packet, $pos-1, $len+1); 
    }
    
    public function _compute_token($password, $scramble){
        if($password == "")
            return false;
            
        $stage1 = sha1($password, TRUE);
        $stage2 = sha1($stage1, TRUE);
        $stage3 = sha1($scramble.$stage2, TRUE);
        
        $bytes = array();
        $chrbytes = "";
        
        for($i = 0; $i < strlen($stage1); $i++) {
            $byte[$i] = ord($stage3[$i]) ^ ord($stage1[$i]);
            $chrbytes .= chr($byte[$i]);
        }
        
        return $chrbytes;
    }

    public function connect($host, $port, $database,$user,$password,$max_packet_size = 0,$pool=NULL,$compact_arrays=NULL){
        if($max_packet_size < 1){
            $max_packet_size = 1024 * 1024;  //default 1 MB
        }
        $this->_max_packet_size = $max_packet_size;
        
        $this->database = $database;
        $this->user = $user;
        $this->host = $host;
        $this->port = $port;
        $this->pool = $pool;
        $this->compact = $compact_arrays;
        
        
        $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($this->socket === false) {
            echo "<p>socket_create() failed: reason: " . socket_strerror(socket_last_error()) . "</p>";
        } else {
            echo "<p>socket successfully created.</p>";
        }
        
        $result = socket_connect($this->socket, $this->host, $this->port);
        if ($result === false) {
            echo "<p>socket_connect() failed.\nReason: ($result) " . socket_strerror(socket_last_error($this->socket)) . "</p>";
        } else {
            echo "<p>successfully connected to $this->host.</p>";
        }
        
        $this->state = STATE_CONNECTED;
        list($packet, $typ)  = $this->_recv_packet();
        
        if($typ == "ERR"){
            list($errno, $msg, $sqlstate) = $this->_parse_err_packet($packet);
            return array(NULL, $msg, $errno, $sqlstate);
        }
        
        $this->protocol_ver = $this->strbyte($packet);
        
        list($server_ver, $pos) = $this->_from_cstring($packet, 2);
        
        if(!$server_ver){
            printf("<p>bad handshake initialization packet: bad server version</p>");
        }
        
        $this->_server_ver = $server_ver;
        
        list($thread_id, $pos) = $this->_get_byte4($packet, $pos);
        
        $scramble = $this->_substr($packet, $pos, $pos + 8 - 1);
        
        if(empty($scramble)){
            printf("<p>1st part of scramble not found</p>");
        }
        
        
        $pos = $pos + 9; //skip filler
        
        //two lower bytes
        list($this->_server_capabilities, $pos) = $this->_get_byte2($packet, $pos);
        
        $this->_server_lang = $this->strbyte($packet, $pos);
        $pos = $pos + 1;
        
        
        list($this->_server_status, $pos) = $this->_get_byte2($packet, $pos);
        
        
        list($more_capabilities, $pos) = $this->_get_byte2($packet, $pos);
                
        $this->_server_capabilities = $this->_server_capabilities | ($more_capabilities << 16);
        
        $len = 21 - 8 - 1;
        $pos = $pos + 1 + 10;
        
        
        $scramble_part2 = $this->_substr($packet, $pos, $pos + $len - 1);
        if(empty($scramble_part2)){
            printf("<p>2nd part of scramble not found</p>");
        }
                
        $scramble = $scramble.$scramble_part2;
        
        $password = $password ? $password : "";
        
        $token = $this->_compute_token($password, $scramble);
        
        
        $client_flags = 260047;
        
        $req = $this->_set_byte4($client_flags).
            $this->_set_byte4($this->_max_packet_size).
            "\0". //-- TODO: add support for charset encoding
            $this->strrep("\0", 23).
            implode("", $this->_to_cstring($user)).
            implode("", $this->_to_binary_coded_string($token)).
            implode("", $this->_to_cstring($database));
        
        $packet_len = 4 + 4 + 1 + 23 + strlen($user) + 1 + strlen($token) + 1 + strlen($database) + 1;
        
        
        $bytes = $this->_send_packet($req, $packet_len);
        
        list($packet, $typ)  = $this->_recv_packet();
        if($typ == "ERR"){
            list($errno, $msg, $sqlstate)  = $this->_parse_err_packet($packet);
            return array(NULL, $msg, $errno, $sqlstate);
        }
        
        if($typ == "EOF"){
            return array(NULL, "old pre-4.1 authentication protocol not supported");
        }
        
        if($typ != "OK"){
            return array(NULL, "bad packet type: ".typ);
        }
        
        $this->state = STATE_CONNECTED;
        
        printf("<p>packet len : %d , %s, connected ok!</p>", strlen($packet), $this->_dump($packet));
        return true;
    }
    
    
    
    public function query($query){
        $bytes = $this->send_query($query);     
        if(empty($bytes)){
            exit("failed to send query");
            return array(null, "failed to send query: ".err);
        }       
        return $this->read_result();
    }
    
    
    public function send_query($query){
        if ($this->state != STATE_CONNECTED){
            return array(null, "cannot send query in the current context: ".self.state);
        }
        
        $this->packet_no = -1;
        $cmd_packet = chr(COM_QUERY).$query;
        $packet_len = 1 + strlen($query);
        
        $bytes = $this->_send_packet($cmd_packet, $packet_len);
        
        $this->state = STATE_COMMAND_SENT;
        return $bytes;
    }
    
    public function  _parse_ok_packet($packet){
        $res = array();
        $pos = 0;
        list($res["affected_rows"], $pos) = $this->_from_length_coded_bin($packet, 2);
        list($res["insert_id"], $pos) = $this->_from_length_coded_bin($packet, $pos);
        list($res["server_status"], $pos) = $this->_get_byte2($packet, $pos);
        list($res["warning_count"], $pos) = $this->_get_byte2($packet, $pos);
        $message = $this->_substr($packet, $pos);
        if($message != ""){
            $res["message"] = $message;
        }
        return $res;
    }
    
    public function _from_length_coded_bin($packet, $pos){
    
        $first = $this->strbyte($packet, $pos);
        if(!$first){
            return array(null, $pos+1);
        }
        
        if($first >= 0 && $first <= 250){
            return array($first, $pos + 1);
        }
        
        if($first == 251){
            return array(null, $pos + 1);
        }
        
        if($first == 252){
            $pos = $pos + 1;
            return $this->_get_byte2($data, $pos);
        }
        
        if($first == 253){
            $pos = $pos + 1;
            return $this->_get_byte3($data, $pos);
        }
        
        if($first == 254){
            $pos = pos + 1;
            return $this->_get_byte8($data, $pos);
        }
        
        return array(NULL, $pos + 1);
    }
    
    
    public function  _parse_result_set_header_packet($packet){
        list($field_count, $pos) = $this->_from_length_coded_bin($packet, 1);
        list($extra, $pos) = $this->_from_length_coded_bin($packet, $pos);      
        return array($field_count, $extra);
    }
    
    public function _recv_field_packet(){
        list($packet, $typ)  = $this->_recv_packet();
        if(!$packet){
            return array(NULL, NULL);
        }
        
        if($typ == "ERR"){
            exit(" _recv_field_packet typ ERR");
        }
        
        if($typ != 'DATA'){
            return array(NULL, "bad field packet type: ".$typ);
        }
        return $this->_parse_field_packet($packet);
    }
    
    public function _parse_field_packet($data){
        $col = array();
        $pos = 0;
        list($catalog, $pos) = $this->_from_length_coded_str($data, 1);
        list($db, $pos) = $this->_from_length_coded_str($data, $pos);
        list($table, $pos) = $this->_from_length_coded_str($data, $pos);
        list($orig_table, $pos) = $this->_from_length_coded_str($data, $pos);
        list($col["name"], $pos) = $this->_from_length_coded_str($data, $pos);
        list($orig_name, $pos) = $this->_from_length_coded_str($data, $pos);
        $pos = $pos + 1;
        list($charsetnr, $pos) = $this->_get_byte2($data, $pos);
        list($length, $pos) = $this->_get_byte4($data, $pos);
        $col["type"] = $this->strbyte($data, $pos);
        return $col;
    }
    
    public function _parse_eof_packet($packet){
        $pos = 2;
        list($warning_count, $pos) = $this->_get_byte2($packet, $pos);
        $status_flags = $this->_get_byte2($packet, $pos);
        return array($warning_count, $status_flags);
    }
    
        
    
    public function _parse_row_data_packet($data, $cols, $compact){ 
        $row = array();
        $pos = 1;
        for($i = 0; $i < count($cols); $i++) {
            list($value, $pos) = $this->_from_length_coded_str($data, $pos);
            $col = $cols[$i];
            $typ = $col["type"];
            $name = $col["name"];
            if($value != null){
                $conv = isset($this->converters[$typ])? $this->converters[$typ] : 0;
                if($conv){
                    $value = $value;
                }
            }
            
            if($compact){
                $row[] = $value;
            }else{
                $row[$name] = $value;
            }

        }       
        return $row;
    }
    
    
    public function read_result(){
        if($this->state != STATE_COMMAND_SENT){
            exit("cannot read result in the current context");
        }

        list($packet, $typ)  = $this->_recv_packet();

        if($typ == "ERR"){
            $this->state = STATE_CONNECTED;
            exit("query error!");
        }
                
        if($typ == 'OK'){
            $res = $this->_parse_ok_packet($packet);
            if($res && $this->_band($res["server_status"], SERVER_MORE_RESULTS_EXISTS) != 0 ){
                exit("again");
            }
            $this->state = STATE_CONNECTED;
            return $res;
        }

        if($typ != 'DATA'){
            $this->state = STATE_CONNECTED;
            exit("packet type not supported");
        }
        
        list($field_count, $extra) = $this->_parse_result_set_header_packet($packet);
        
        $cols  = array();
        for($i = 0; $i < $field_count; $i++) {
            //printf("<p>field_count i: %d </p>", $i);
            $col = $this->_recv_field_packet();
            if(!$col){
                return NULL;
            }
            $cols[$i] = $col;
        }
        
        list($packet, $typ) = $this->_recv_packet();
        
        
        if($typ != 'EOF'){
            return array(NULL, "unexpected packet type ".typ." while eof packet is "."expected");
        }
        
        $compact = $this->compact;
        
        $rows = array();
        
        while (true){
            list($packet, $typ) = $this->_recv_packet();
            
            if($typ == 'EOF'){
                list($warning_count, $status_flags) = $this->_parse_eof_packet($packet);    
                if($this->_band($status_flags, SERVER_MORE_RESULTS_EXISTS) != 0){
                    return array($rows, "again");
                }
                break;
            }
            
            $row = $this->_parse_row_data_packet($packet, $cols, $compact);
            $rows[] = $row;
        }
        $this->state = STATE_CONNECTED;
        return $rows;
    }

}
