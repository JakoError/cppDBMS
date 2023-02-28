C/C++实现简易的DBMS 
simple DBMS(Database Management System) Based on C/C++

## 项目包含：

   1. 客户端：
      1. 发出结构化命令到服务端
         1. 使用boost asio库发送SQL命令
      2. 接受服务端返回的结构化内容
      3. 将服务端返回的结构化数据处理打印
   2. 服务端:
      1. 负责管理存储文件格式
         1. 表信息文件
         2. 表数据文件
         3. 索引文件
      2. 接受客户端收到的结构化命令
         1. 处理查询命令输入（包含错误）
         2. 正则识别
      3. 层级调用功能实现对应文件操作并获取命令结果
      4. 处理发送命令结果到io

## Project includes:

   1. Client:
       1. Issue structured commands to the server
          1. Use the boost asio library to send SQL commands
       2. Accept the structured content returned by the server
       3. Process and print the structured data returned by the server
   2. Server:
       1. Responsible for managing storage file formats
          1. Table information file
          2. Table data file
          3. Index file
       2. Accept the structured commands received by the client
          1. Handle query command input (including errors)
          2. Regular recognition
       3. Hierarchical call function to implement corresponding file operations and obtain command results
       4. Process and send command results to io

注意：本项目依赖于Boost库，请修改CMakeLists.txt中：
Reset the BOOST profile in CMakeLists.txt for functional usage.
```
set(BOOST_INC_DIR  D:/Codes/C++/boost_1_80_0)
set(BOOST_LINK_DIR  D:/Codes/C++/boost_1_80_0/stage/lib)
```
`D:/Codes/C++/boost_1_80_0` 修改为本地所在的Boost库位置
Modify this to the Boost library location in your env.


本项目为bzj@UESTC命题约束提供
Bzj@UESTC provided this project
## Credits

- Boost - https://github.com/boostorg/boost
