#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <filesystem>
#include "concurrentqueue.h"
#include <cstdio>
#include <string.h>
#include <Windows.h>
#include <WinBase.h>
#include <tchar.h>
#include "Utils.h"
#include "FileProcessors/ConcurrentFileProcessor.h"
#include "FileProcessors/OpenHandleConcurrentFileProcessor.h"
#include "FileProcessors/OpenHandleFileProcessor.h"
#include "FileProcessors/SequentialFileProcessor.h"
#include "FileProcessors/PipelinedFileProcessor.h"

using namespace std;
namespace fs = std::filesystem;

void CreateTestFiles();
void SetupTestFolders();

int main()
{
	cout << "Setup test folders...";
    SetupTestFolders();
	cout << " Complete" << endl;

	cout << "Create Test Files...";
    CreateTestFiles();
	cout << " Complete" << endl;
 
    //Single threaded
	
	cout << "Single Threaded Parse Test Files...";
    ParseTestFiles();
	cout << " Complete" << endl;

	/*
    //With open handles
    //ParseTestFilesWithOpenHandle();
	*/

    //Multi threaded
	/*cout << "Multi-threaded Parse Test Files...";
    ParseTestFilesConcurrent();
	cout << " Complete" << endl;*/

	//Pipelined
	cout << "Pipelined Parse Test Files...";
	ParseTestFilesPipelined();
	cout << " Complete" << endl;
	
	/*
	// Multi threaded with open handle
	cout << "Multi-threaded Parse Test Files with open handles...";
	ParseTestFilesWithOpenHandleParallel();
	cout << " Complete" << endl;
	*/
}

void DeleteDirectory(const string& path)
{
	WIN32_FIND_DATAA ffd;
	string searchPath = path + "\\*";
	HANDLE hFind = FindFirstFileA(searchPath.c_str(), &ffd);

	if (hFind == INVALID_HANDLE_VALUE)
	{
		cout << "Error finding files to delete in " << path << ": " << GetLastError() << endl;
		return;
	}

	do
	{
		string fileName = ffd.cFileName;
		if (fileName == "." || fileName == "..")
		{
			continue;
		}

		string filePath = path + "\\" + fileName;

		if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
		{
			// Recursively delete subdirectory
			DeleteDirectory(filePath);
		}
		else
		{
			// Delete file
			if (!DeleteFileA(filePath.c_str()))
			{
				cout << "Error deleting file " << filePath << ": " << GetLastError() << endl;
			}
		}
	}
	while (FindNextFileA(hFind, &ffd) != 0);

	if (GetLastError() != ERROR_NO_MORE_FILES)
	{
		cout << "Error finding files to delete in " << path << ": " << GetLastError() << endl;
	}

	FindClose(hFind);

	// Delete directory itself
	if (!RemoveDirectoryA(path.c_str()))
	{
		cout << "Error deleting directory " << path << ": " << GetLastError() << endl;
	}
}

void SetupTestFolders()
{
	// Directories to remove
	vector<string> toRemove = {
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\CreateTestFiles",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFiles",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\Output",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesConcurrent",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesPipelined"
	};

	// Directories to add
	vector<string> toAdd = {
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\CreateTestFiles",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\Output",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFiles",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFiles\\move_dir",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\Output\\move_dir",
		//"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles\\move_dir",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesConcurrent",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesConcurrent\\move_dir",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel\\move_dir",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesPipelined",
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesPipelined\\move_dir"
	};

	for (const auto& path : toRemove)
	{
		DeleteDirectory(path);
	}

	for (const auto& path : toAdd)
	{
		if (!CreateDirectoryA(path.c_str(), NULL))
		{
			cout << "Error creating directory " << path << ": " << GetLastError() << endl;
		}
	}
}

void CreateTestFiles()
{
    int numFiles = 1000; // Number of files to generate
    int wordsPerFile = 1000; // Number of words per file
    string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\CreateTestFiles\\"; // Directory to save files in

    for (int i = 1; i <= numFiles; i++) 
	{
        stringstream fileName;
        fileName << directory << "file" << i << ".txt"; // Generate file name
        ofstream outFile(fileName.str()); // Open output file
        if (outFile.is_open()) 
		{
            for (int j = 1; j <= wordsPerFile; j++) 
			{
                outFile << "word" << rand() % 100 << " "; // Write a random word
            }
            outFile.close(); // Close output file
        }
        else 
		{
            cout << "Unable to create file " << fileName.str() << endl;
        }
    }
}

