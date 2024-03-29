﻿Option Compare Database

Public Sub CreateTable()
    On Error Resume Next
    Application.CurrentDb.Execute "Drop Table [TestData];"
    On Error GoTo 0

    Dim con As ADODB.Connection
    Set con = CurrentProject.Connection
    con.Execute "" _
        & "CREATE TABLE [TestData](" _
            & " [id]                  INTEGER NOT NULL" _
            & ",[first_name]          VARCHAR(50) NOT NULL" _
            & ",[last_name]           VARCHAR(50) NOT NULL" _
            & ",[email]               VARCHAR(50) NULL" _
            & ",[gender]              VARCHAR(50) NOT NULL" _
            & ",[image]               IMAGE NOT NULL" _
            & ",[salary]              DECIMAL(7,2) NULL" _
            & ",CONSTRAINT [PrimaryKey] PRIMARY KEY ([id])" _
        & ");"

    con.Execute "CREATE INDEX [IX_LastName_Gender] ON [TestData]([last_name],[gender]);"

    Set con = Nothing
End Sub

Public Sub CreateData()
    Dim con As ADODB.Connection
    Set con = CurrentProject.Connection
    
   Dim strFile As String, strLine As String
   strFile = "C:\Temp\Inserts.sql"
   Open strFile For Input As #1
   Do Until EOF(1)
      Line Input #1, strLine
      
      con.Execute strLine
   Loop
    
    
    Set con = Nothing
End Sub
 