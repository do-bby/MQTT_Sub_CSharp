using System;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using MySql.Data.MySqlClient;
using System.Text;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DotNetty.Common.Utilities;
using System.Text.RegularExpressions;
class Program
{
    //static int id = 1;
    static string uID;
    static async Task InituID()
    {
        uID = await getUID();
    }
    static async Task Main(string[] args)
    {
        string brokerAddress = "139.150.83.249";
        int brokerPort = 1883;
        await InituID();
        string topic = "PohangPG/#";
        string username = "root";
        string password = "public";
        int status = 0;

        //Pohang login api : https://pohang.ictpeople.co.kr/api/User/login
        //if (status == 200) 일 때 subscribe
        string url = "https://pohang.ictpeople.co.kr/api/User/login";
        string postData = "{\"username\":\"openapiuser@gmail.com\",\"password\":\"AQAAAAEAACcQAAAAEGkDt7W5HYOCORwYFD0oZk3nG7R/klqVO6Zr5IKCEEsf9ElsGcysBvO0vAZsbPCd1g==\"}";

        HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
        request.Method = "POST";
        request.ContentType = "application/json";
        request.Accept = "application/json";
        byte[] byteArray = Encoding.UTF8.GetBytes(postData);
        request.ContentLength = byteArray.Length;

        using (Stream dataStream = request.GetRequestStream())
        {
            dataStream.Write(byteArray, 0, byteArray.Length);
        }

        try
        {
            using (WebResponse response = request.GetResponse())
            {
                using (Stream responseStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                    string responseStr = reader.ReadToEnd();
                    dynamic res = Newtonsoft.Json.Linq.JObject.Parse(responseStr);
                    status = res.statusCode;
                    Console.WriteLine("Status Code: " + status);
                }
            }
        }
        catch (WebException ex)
        {
            using (WebResponse response = ex.Response)
            {
                HttpWebResponse httpResponse = (HttpWebResponse)response;
                Console.WriteLine("Error Response Code: " + (int)httpResponse.StatusCode);
            }
        }
        if(status == 200){
            MqttClient client = new MqttClient(brokerAddress, brokerPort, false, null, null, MqttSslProtocols.None);
            //string topic = "AASX/#" => AASX 하위 모든 서브모델을 Subscribe, AASX/+ 한단계 하위 까지만
            // Subscribe to the topic
            client.Subscribe(new string[] { topic }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
            
            client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
            client.Connect(Guid.NewGuid().ToString(),username,password);
            
            Console.Write("Connect Success");
        
            Console.WriteLine($"Subscribed to topic: {topic}");
            Console.Write("Recevied Success");
            Console.ReadLine();
            client.Disconnect();
        }

    }
    static string extract(string topic){
        Regex regex = new Regex(@"/(.*?)/");
        Match match = regex.Match(topic);
        if (match.Success)
        {
            return match.Groups[1].Value;
        }
        else
        {
            return null;
        }
    }
    static async Task<string> getUID(){
        using(HttpClient client = new HttpClient())
        {
            string url = "https://pohang.ictpeople.co.kr/api/Equipment/GetEquipment?SerialNo=DX20240220-0001";
            HttpResponseMessage res = await client.GetAsync(url);
            try{
                HttpResponseMessage response = await client.GetAsync(url);
                if(response.IsSuccessStatusCode){
                    string responseBody = await response.Content.ReadAsStringAsync();
                    JObject json = JObject.Parse(responseBody);
                    JArray arr = (JArray)json["data"];
                    JObject obj = (JObject) arr[0];
                    string data = (string)obj["serialNo"];
                    return data;
                }
                else{
                    return null;
                }
            } catch(HttpRequestException e){
                Console.WriteLine("fail" + e.Message);
                return null;
            }
        }
    }

    private static void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
    {
        string message = System.Text.Encoding.UTF8.GetString(e.Message);
        string topic = e.Topic;
        Console.WriteLine($"Received message: {message} on topic : {topic}");
        InsertDB(topic,e.Message);
        UpdateDB(topic,message);
        if(topic.Contains("Ncst")){
            uncstInsert(topic,message);
        }
        else if(topic.Contains("Fcst")){
            ufcstInsert(topic,message);
        }
        else if(topic.Contains("VF")){
            vfcstInsert(topic,message);
        }
        else if(topic.Contains("uv") || topic.Contains("atmo")){
            uvindexInsert(topic,message);
        }
        else if(topic.Contains("wave")){
            waveInsert(topic,message);
        }
        else if(topic.Contains("WthrWrnMsg")){
            wthrWrnMsgInsert(topic,message);
        }
        else if (topic.Contains("PreTab")){
            tideObsPreTabInsert(topic,message);            
        }
        else if(topic.Contains("LunPhInfo")){
            lunphinfoInsert(topic,message);
        }
        else if(topic.Contains("ocean")){
            oceanInsert(topic,message);
        }
    }
    //Insert table data
    private static void InsertDB(string topic,byte[] message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=emqx_data";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO emqx_messages (clientid,topic,payload,created_at) VALUES (@value1,@value2,@value3,NOW())";
                MySqlCommand command = new MySqlCommand(query,con);                
                //command.Parameters.AddWithValue("@value1", id);
                //id++;
                command.Parameters.AddWithValue("@value1", extract(topic));
                command.Parameters.AddWithValue("@value2", topic);
                command.Parameters.AddWithValue("@value3", message);
                int rowsAffected = command.ExecuteNonQuery();
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }

    //Update table data
    private static void UpdateDB(string topic,string message){
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=emqx_data";
        MySqlConnection con = new MySqlConnection(connectionString);        
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string checkQuery = "SELECT * FROM emqx_messages_master WHERE clientid = @clientid AND topic = @topic";
                string updateQuery = "UPDATE emqx_messages_master SET payload = @payload, latest_time = @latest_time WHERE clientid = @clientid AND topic = @topic";
                string insertQuery = "INSERT INTO emqx_messages_master (clientid,topic,payload,latest_time) VALUES (@clientid,@topic,@payload,@latest_time)";
                using(MySqlCommand command = new MySqlCommand(checkQuery,con)){
                    command.Parameters.AddWithValue("@clientid",uID);
                    command.Parameters.AddWithValue("@topic",topic);
                    using(MySqlDataReader reader = command.ExecuteReader()){
                        if(reader.Read()){
                            reader.Close();
                            using (MySqlCommand updateCmd = new MySqlCommand(updateQuery, con))
                            {
                                updateCmd.Parameters.AddWithValue("@payload", message);
                                updateCmd.Parameters.AddWithValue("@latest_time", DateTime.Now);
                                updateCmd.Parameters.AddWithValue("@clientid", extract(topic));
                                updateCmd.Parameters.AddWithValue("@topic", topic);
                                int rows = updateCmd.ExecuteNonQuery();
                                if (rows > 0)
                                {
                                    Console.WriteLine("Update success");
                                }
                                else
                                {
                                    Console.WriteLine("Update fail");
                                }
                            }
                        }
                        else{
                            reader.Close();
                            using (MySqlCommand insertCmd = new MySqlCommand(insertQuery, con))
                            {
                                insertCmd.Parameters.AddWithValue("@clientid", extract(topic));
                                insertCmd.Parameters.AddWithValue("@topic", topic);
                                insertCmd.Parameters.AddWithValue("@payload", message);
                                insertCmd.Parameters.AddWithValue("@createdAt", DateTime.Now);
                                int rows = insertCmd.ExecuteNonQuery();
                                if (rows > 0)
                                {
                                    Console.WriteLine("Insert success");
                                }
                                else
                                {
                                    Console.WriteLine("Insert fail");
                                }
                            }
                        }
                    }
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    //초단기 실황 data
    private static void uncstInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_UltraSrtNcst (obsrvalue, basedate, basetime, nx, ny, category, created) " +
               "VALUES (@obsrvalue, @basedate, @basetime, @nx, @ny, @category, NOW()) " +
               "ON DUPLICATE KEY UPDATE obsrvalue = VALUES(obsrvalue), nx = VALUES(nx), ny = VALUES(ny), modified = NOW()";
                MySqlCommand command = new MySqlCommand(query,con);
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@obsrvalue", data["obsrValue"]);
                command.Parameters.AddWithValue("@basedate", data["baseDate"]);
                command.Parameters.AddWithValue("@basetime", data["baseTime"]);
                command.Parameters.AddWithValue("@nx", data["nx"]);
                command.Parameters.AddWithValue("@ny", data["ny"]);
                command.Parameters.AddWithValue("@category", data["category"]);
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    //초단기 예보 data
    private static void ufcstInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_UltraSrtFcst (fcstvalue, fcstdate, fcsttime, basedate, basetime, nx, ny, category,created,modified) VALUES (@fcstvalue, @fcstdate, @fcsttime, @basedate, @basetime, @nx, @ny, @category,NOW(),NOW()) " +
                "ON DUPLICATE KEY UPDATE fcstvalue = VALUES(fcstvalue), fcsttime = VALUES(fcsttime), fcstdate = VALUES(fcstdate), nx = VALUES(nx), ny = VALUES(ny)" +
                ", modified = NOW()";
                MySqlCommand command = new MySqlCommand(query,con);
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@fcstvalue", data["fcstValue"]);
                command.Parameters.AddWithValue("@fcstdate", data["fcstDate"]);
                command.Parameters.AddWithValue("@fcsttime", data["fcstTime"]);
                command.Parameters.AddWithValue("@basedate", data["baseDate"]);
                command.Parameters.AddWithValue("@basetime", data["baseTime"]);
                command.Parameters.AddWithValue("@nx", data["ny"]);
                command.Parameters.AddWithValue("@ny", data["ny"]);
                command.Parameters.AddWithValue("@category", data["category"]);
                int rowsAffected = command.ExecuteNonQuery();
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    //단기 예보 data
    private static void vfcstInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_VilageFcst (fcstvalue, fcstdate, fcsttime, basedate, basetime, nx, ny, category,created,modified) VALUES (@fcstvalue, @fcstdate, @fcsttime, @basedate, @basetime, @nx, @ny, @category,NOW(),NOW()) " +
                "ON DUPLICATE KEY UPDATE fcstvalue = VALUES(fcstvalue), fcsttime = VALUES(fcsttime), fcstdate = VALUES(fcstdate), nx = VALUES(nx), ny = VALUES(ny) " +
                ", modified = NOW()";
                MySqlCommand command = new MySqlCommand(query,con);
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@fcstvalue", data["fcstValue"]);
                command.Parameters.AddWithValue("@fcstdate", data["fcstDate"]);
                command.Parameters.AddWithValue("@fcsttime", data["fcstTime"]);
                command.Parameters.AddWithValue("@basedate", data["baseDate"]);
                command.Parameters.AddWithValue("@basetime", data["baseTime"]);
                command.Parameters.AddWithValue("@nx", data["ny"]);
                command.Parameters.AddWithValue("@ny", data["ny"]);
                command.Parameters.AddWithValue("@category", data["category"]);
                int rowsAffected = command.ExecuteNonQuery();
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    //uvindex + atmo data
    private static void uvindexInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                if(topic.Contains("uv")){
                    con.Open();
                    Console.WriteLine("DB Connected");
                    string query = "INSERT INTO API_UVINDEX (code, areaNo, date, msg,created,modified) VALUES (@code, @areaNo, @date, @msg, NOW(),NOW()) " +
                    "ON DUPLICATE KEY UPDATE code = VALUES(code), msg = VALUES(msg)" +
                    ", modified = NOW()";
                    MySqlCommand command = new MySqlCommand(query,con);
                    JObject data = JObject.Parse(message);
                    command.Parameters.AddWithValue("@code", data["code"]);
                    command.Parameters.AddWithValue("@areaNo", data["areaNo"]);
                    command.Parameters.AddWithValue("@date", data["date"]);
                    command.Parameters.AddWithValue("@msg", data);

                    int rowsAffected = command.ExecuteNonQuery();
                    if(rowsAffected > 0){
                        Console.WriteLine("insert success");
                    }
                    else{
                        Console.WriteLine("insert fail");
                    }    
                }
                else if(topic.Contains("atmo")){
                    con.Open();
                    Console.WriteLine("DB Connected");
                    string query = "INSERT INTO API_ATMO (code, areaNo, date, msg,created,modified) VALUES (@code, @areaNo, @date, @msg, NOW(),NOW()) " +
                    "ON DUPLICATE KEY UPDATE code = VALUES(code), msg = VALUES(msg)" +
                    ", modified = NOW()";
                    MySqlCommand command = new MySqlCommand(query,con);
                    JObject data = JObject.Parse(message);
                    command.Parameters.AddWithValue("@code", data["code"]);
                    command.Parameters.AddWithValue("@areaNo", data["areaNo"]);
                    command.Parameters.AddWithValue("@date", data["date"]);
                    command.Parameters.AddWithValue("@msg", data);

                    int rowsAffected = command.ExecuteNonQuery();
                    if(rowsAffected > 0){
                        Console.WriteLine("insert success");
                    }
                    else{
                        Console.WriteLine("insert fail");
                    }  
                }
                
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }

    //해수욕장 파고 API
    private static void waveInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_WAVE (beachNum, tm, wh,created,modified) " +
               "VALUES (@beachNum, @tm, @wh, NOW(), NOW()) " +
               "ON DUPLICATE KEY UPDATE obsrvalue = VALUES(beachNum), tm = VALUES(tm), wh = VALUES(wh), modified = NOW()";
                MySqlCommand command = new MySqlCommand(query,con);
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@beachNum", data["beachNum"]);
                command.Parameters.AddWithValue("@tm", data["tm"]);
                command.Parameters.AddWithValue("@th", data["th"]);
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    private static void wthrWrnMsgInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_WthrWrnMsg (stnId, tmFc, tmSeq, area, warFc, title, msg, Fermentationtime, Specialfermentationtime, Specialmsg, Prespecialreport, other, created, modified) " +
               "VALUES (@stnId, @tmFc, @tmSeq, @area, @warFc, @title, @msg, @Fermentationtime, @Specialfermentationtime, @Specialmsg, @Prespecialreport, @other, NOW(), NOW()) " +
               "ON DUPLICATE KEY UPDATE stnId = VALUES(stnId), tmSeq = VALUES(tmSeq), area = VALUES(area), warFc = VALUES(warFc), " + 
               "title = VALUES(title), msg = VALUES(msg), Fermentationtime = VALUES(Fermentationtime), Specialfermentationtime = VALUES(Specialfermentationtime), Specialmsg = VALUES(Specialmsg), Prespecialreport = VALUES(Prespecialreport), other = VALUES(other), modified = NOW()";

                MySqlCommand command = new MySqlCommand(query,con);
                
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@stnId", data["stnId"]);
                Object obj = data["tmFc"];
                string t = Convert.ToString(obj);
                DateTime tmDate = DateTime.ParseExact(t,"yyyyMMddHHmm",null);
                command.Parameters.AddWithValue("@tmFc", tmDate);
                Console.WriteLine(data["t2"].ToString());
                command.Parameters.AddWithValue("@tmSeq", data["tmSeq"]);
                command.Parameters.AddWithValue("@area", data["t2"].ToString());
                command.Parameters.AddWithValue("@warFc", data["warFc"]);
                command.Parameters.AddWithValue("@title", data["t1"].ToString());
                command.Parameters.AddWithValue("@msg", data["t4"].ToString());
                command.Parameters.AddWithValue("@Fermentationtime", data["t3"].ToString());
                Object obj2 = data["t5"];
                string t2 = Convert.ToString(obj2);
                DateTime tmDate2 = DateTime.ParseExact(t,"yyyyMMddHHmm",null);
                command.Parameters.AddWithValue("@Specialfermentationtime", tmDate2);
                command.Parameters.AddWithValue("@Specialmsg", data["t6"].ToString());
                command.Parameters.AddWithValue("@Prespecialreport", data["t7"].ToString());
                command.Parameters.AddWithValue("@other", data["other"].ToString());
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }

    //최신 ocean
    private static void oceanInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_ObsRecent (OBS_POST_ID,OBS_POST_NM,OBS_LAT,OBS_LON,OBS_LAST_REQ_CNT,RECODE_TIME,WIND_DIR,WIND_SPEED,WIND_GUST,AIR_TEMP,AIR_PRESS,WATER_TEMP,TIDE_LEVEL,SALINITY,CreatedDate,CreatedBy,ModifiedDate,ModifiedBy) VALUES (@OBS_POST_ID,@OBS_POST_NM,@OBS_LAT,@OBS_LON,@OBS_LAST_REQ_CNT,@RECODE_TIME,@WIND_DIR,@WIND_SPEED,@WIND_GUST,@AIR_TEMP,@AIR_PRESS,@WATER_TEMP,@TIDE_LEVEL,@SALINITY,NOW(),@CreatedBy,NOW(),@ModifiedBy)" +
                "ON DUPLICATE KEY UPDATE OBS_POST_NM = VALUES(OBS_POST_NM), OBS_LAT = VALUES(OBS_LAT),OBS_LON = VALUES(OBS_LON),OBS_LAST_REQ_CNT = VALUES(OBS_LAST_REQ_CNT),WIND_DIR = VALUES(WIND_DIR),WIND_SPEED = VALUES(WIND_SPEED),WIND_GUST = VALUES(WIND_GUST),AIR_TEMP = VALUES(AIR_TEMP),AIR_PRESS = VALUES(AIR_PRESS),WATER_TEMP = VALUES(WATER_TEMP),TIDE_LEVEL = VALUES(TIDE_LEVEL),SALINITY = VALUES(SALINITY)," +
                "ModifiedDate = NOW()";

                MySqlCommand command = new MySqlCommand(query,con);
                
                JObject obj = JObject.Parse(message);
                JObject data = (JObject)obj["data"];
                JObject meta = (JObject)obj["meta"];
                command.Parameters.AddWithValue("@OBS_POST_ID", meta["obs_post_id"]);
                command.Parameters.AddWithValue("@OBS_POST_NM",meta["obs_post_name"]);
                command.Parameters.AddWithValue("@OBS_LAT", meta["obs_lat"]);
                command.Parameters.AddWithValue("@OBS_LON", meta["obs_lon"]);
                command.Parameters.AddWithValue("@OBS_LAST_REQ_CNT", meta["obs_last_req_cnt"]);
                command.Parameters.AddWithValue("@RECODE_TIME", data["record_time"]);
                command.Parameters.AddWithValue("@WIND_DIR", data["wind_dir"]);
                command.Parameters.AddWithValue("@WIND_SPEED", data["wind_speed"]);
                command.Parameters.AddWithValue("@WIND_GUST", data["wind_gust"]);
                command.Parameters.AddWithValue("@AIR_TEMP", data["air_temp"]);
                command.Parameters.AddWithValue("@AIR_PRESS", data["air_press"]);
                command.Parameters.AddWithValue("@WATER_TEMP", data["water_temp"]);
                command.Parameters.AddWithValue("@TIDE_LEVEL", data["tide_level"]);
                command.Parameters.AddWithValue("@SALINITY", data["Salinity"]);
                // command.Parameters.AddWithValue("@SALINITY", data["CreatedBy"]);
                // command.Parameters.AddWithValue("@SALINITY", data["ModifiedBy"]);
            

                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }

    //해양 기온,기압,풍향,풍속,수온 조위실제/관측 데이터 oceandataInsert
    private static void oceandataInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                if(topic.Contains("tideCurPre")){
                    String query = "INSERT INTO API_tideCurPre (obs_id, obs_name, obs_lat, obs_lon, record_time, real_value, pre_value, obs_last_req_cnt, modified, created) VALUES (@obs_id,@obs_name,@obs_lat,@obs_lon,@record_time,@real_value,@pre_value,@obs_last_req_cnt, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                        "real_value = VALUES(real_value), pre_value = VALUES(pre_value), obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";
                        MySqlCommand command = new MySqlCommand(query,con);
                
                        JObject obj = JObject.Parse(message);
                        JObject data = (JObject)obj["data"];
                        JObject meta = (JObject)obj["meta"];
                        command.Parameters.AddWithValue("@obs_id", meta["obs_post_id"]);
                        command.Parameters.AddWithValue("@obs_name",meta["obs_post_name"]);
                        command.Parameters.AddWithValue("@obs_lat", meta["obs_lat"]);
                        command.Parameters.AddWithValue("@obs_lon", meta["obs_lon"]);
                        command.Parameters.AddWithValue("@tph_time", data["record_time"]);
                        command.Parameters.AddWithValue("@tph_level", data["real_value"]);
                        command.Parameters.AddWithValue("@hl_code", data["pre_value"]);
                        command.Parameters.AddWithValue("@obs_last_req_cnt", meta["obs_last_req_cnt"]);
                        int rowsAffected = command.ExecuteNonQuery();
                        Console.WriteLine(rowsAffected);
                        if(rowsAffected > 0){
                            Console.WriteLine("insert success");
                        }
                        else{
                            Console.WriteLine("insert fail");
                        }
                }
                else if(topic.Contains("")){
                    
                }
                            
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }


    //조석
    private static void tideObsPreTabInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_tideObsPreTab (obs_id, obs_name, obs_lat, obs_lon, tph_time, tph_level, hl_code, obs_last_req_cnt, modified, created) " +
                "VALUES (@obs_id, @obs_name, @obs_lat, @obs_lon, @tph_time, @tph_level, @hl_code, @obs_last_req_cnt, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE obs_name = VALUES(obs_name), obs_lat = VALUES(obs_lat), obs_lon = VALUES(obs_lon), " +
                "tph_time = VALUES(tph_time), tph_level = VALUES(tph_level), hl_code = VALUES(hl_code), " +
                "obs_last_req_cnt = VALUES(obs_last_req_cnt), modified = NOW()";

                MySqlCommand command = new MySqlCommand(query,con);
                
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@obs_id", data["obs_id"]);
                command.Parameters.AddWithValue("@obs_name",data["obs_name"]);
                command.Parameters.AddWithValue("@obs_lat", data["obs_lat"]);
                command.Parameters.AddWithValue("@obs_lon", data["obs_lon"]);
                command.Parameters.AddWithValue("@tph_time", data["tph_time"]);
                command.Parameters.AddWithValue("@tph_level", data["tph_level"]);
                command.Parameters.AddWithValue("@hl_code", data["hl_code"]);
                command.Parameters.AddWithValue("@obs_last_req_cnt", data["obs_last_req_cnt"]);
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }
    //월령
    private static void lunphinfoInsert(string topic,string message)
    {
        string connectionString = "server=139.150.83.249;port=3306;user=root;password=public;database=pohang";
        MySqlConnection con = new MySqlConnection(connectionString);      
        try
            {
                con.Open();
                Console.WriteLine("DB Connected");
                string query = "INSERT INTO API_lunPhInfo (lunAge,sol_time, modified, created) " +
                "VALUES (@lunAge, @sol_time, NOW(), NOW()) " +
                "ON DUPLICATE KEY UPDATE lunAge = VALUES(lunAge), sol_time = VALUES(sol_time),modified = NOW()";

                MySqlCommand command = new MySqlCommand(query,con);
                
                JObject data = JObject.Parse(message);
                command.Parameters.AddWithValue("@obs_id", data["lunAge"]);
                string year = (string)data["solYear"];
                string month = (string)data["solMonth"];
                string day = (string)data["solDay"];
                DateTime sol_time = new DateTime(Int32.Parse(year),Int32.Parse(month),Int32.Parse(day));
                command.Parameters.AddWithValue("@sol_time",sol_time);                
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine(rowsAffected);
                if(rowsAffected > 0){
                    Console.WriteLine("insert success");
                }
                else{
                    Console.WriteLine("insert fail");
                }
            } catch(Exception e){
                Console.WriteLine("error :" + e.Message);
            }
            finally
            {
                if (con.State == System.Data.ConnectionState.Open)
                {
                    con.Close();
                }
            }
    }

}

