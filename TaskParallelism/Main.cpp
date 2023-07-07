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

using namespace std;
namespace fs = std::filesystem;

void ParseTestFilesWithOpenHandle();
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
	cout << "Multi-threaded Parse Test Files...";
    ParseTestFilesConcurrent();
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
		"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel\\move_dir"
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



std::wstring StringToWideString(const std::string& str)
{
	int length = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), -1, NULL, 0);
	std::wstring result(length, 0);
	MultiByteToWideChar(CP_UTF8, 0, str.c_str(), -1, &result[0], length);
	return result;
}

__declspec(noinline) void ParseTestFilesWithOpenHandle()
{
	unordered_map<string, int> wordFreq; // To keep track of word frequency

	std::string directory = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\CreateTestFiles\\"; // Directory containing input files
	std::vector<std::string> fileNames;

	// Get all file paths in the directory
	for (const auto& entry : fs::directory_iterator(directory))
	{
		if (entry.is_regular_file())
		{
			fileNames.push_back(entry.path().string());
		}
	}

	char buf[256];

	for (const auto& fileName : fileNames)
	{
		ifstream inFile(fileName); // Open input file
		if (inFile.is_open())
		{
			// Read the entire file into a string.
			std::stringstream ss;
			ss << inFile.rdbuf();
			std::string fileContents = ss.str();

			const char* s = fileContents.c_str();
			while (*s != '\0')
			{
				int n = std::sscanf(s, "%255s", buf);
				if (n == 1)
				{
					s += std::strlen(buf) + 1;
					++wordFreq[buf];
				}
				else
				{
					++s;
				}
			}
			inFile.close(); // Close input file
		}
		else
		{
			cout << "Unable to open file " << fileName << endl;
		}
	}

	vector<pair<string, int>> wordFreqVec(wordFreq.begin(), wordFreq.end()); // Convert word frequency map to vector

	sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b)
		{ // Sort vector in descending order of word frequency
			return a.second > b.second;
		});

	DWORD sectorSize = Utils::GetSectorSize();

	std::vector<HANDLE> handles(fileNames.size());
    std::vector<string> toPaths;

	void* pMemory = VirtualAlloc(NULL,sectorSize, MEM_COMMIT, PAGE_READWRITE);
	if (!pMemory)
	{
		std::cerr << "Failed to allocate memory: " << GetLastError() << std::endl;
		return;
	}

	for (size_t i = 0; i < fileNames.size(); ++i)
	{
        string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles\\top_words_" + std::to_string(i) + ".txt";

		std::string toPath = path;
		size_t pos = toPath.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
            toPaths.emplace_back(std::move(toPath));
		}

		std::wstring wpath(path.begin(), path.end());
		DWORD dwDesiredAccess = GENERIC_WRITE | DELETE;
        handles[i] = CreateFileW(wpath.c_str(), 
			dwDesiredAccess,
			0, 
			NULL, 
			CREATE_ALWAYS, 
			FILE_FLAG_NO_BUFFERING,
			NULL); // Open output file

		if (handles[i] != INVALID_HANDLE_VALUE)
		{
			int count = 0;

			std::string contents;
			for (const auto& wordFreqPair : wordFreqVec)
			{
				count++;
				contents += wordFreqPair.first + ": " + std::to_string(wordFreqPair.second) + "\r\n";
				if (count == 10)
				{
					// Limit to top 10 words
					break;
				}
			}
			
			memcpy(pMemory, contents.c_str(), contents.size());

			DWORD bytesWritten = 0;
			if(!WriteFile(handles[i], pMemory, sectorSize, &bytesWritten, NULL)) // Write top words to output file
				std::cout << "Error writing to file: " << GetLastError() << std::endl;

		}
		else
		{
			std::cout << "Unable to open output file" << std::endl;
		}
	}

    
	if (!VirtualFree(pMemory, 0, MEM_RELEASE))
	{
		std::cerr << "Failed to free memory: " << GetLastError() << std::endl;
		return;
	}


	std::wstring source_file_w;
	for (size_t i = 0; i < handles.size(); ++i)
	{
		/*FILE_IO_PRIORITY_HINT_INFO hint = {IoPriorityHintVeryLow};
		DWORD bytesReturned = 0;

		BOOL result = GetFileInformationByHandleEx(handles[i], FileIoPriorityHintInfo, &hint, sizeof(hint));
		if (result)
		{
			switch (hint.PriorityHint)
			{
			case IoPriorityHintVeryLow:
				// Handle very low priority hint
				break;
			case IoPriorityHintLow:
				// Handle low priority hint
				break;
			case IoPriorityHintNormal:
				// Handle normal priority hint
				break;
			default:
				// Handle unknown priority hint
				break;
			}
		}
		else
			std::cout << "Error getting priority hint info on file: " << GetLastError() << std::endl;*/

		/*FILE_IO_PRIORITY_HINT_INFO hint = {IoPriorityHintNormal};
		if (!SetFileInformationByHandle(handles[i], FileIoPriorityHintInfo, &hint, sizeof(hint)))
			std::cout << "Error setting priority hint info on file: " << GetLastError() << std::endl;*/
		
		typedef struct _FILE_RENAME_INFORMATION
		{
			BOOLEAN ReplaceIfExists;
			HANDLE RootDirectory;
			ULONG FileNameLength;
			WCHAR FileName[1];
		} FILE_RENAME_INFORMATION, * PFILE_RENAME_INFORMATION;

        source_file_w = StringToWideString(toPaths[i]);

		size_t namesize = (wcslen(source_file_w.c_str())+1) * sizeof(wchar_t);
		size_t infosize = sizeof(FILE_RENAME_INFORMATION) + namesize;
		FILE_RENAME_INFORMATION* RenameInfo = (FILE_RENAME_INFORMATION*) _alloca(infosize);
		memset(RenameInfo, 0, infosize);
		RenameInfo->ReplaceIfExists = TRUE;
		RenameInfo->RootDirectory = 0;
		RenameInfo->FileNameLength = namesize;
		memcpy(RenameInfo->FileName, source_file_w.c_str(), namesize);

		/*DWORD transferSize;
		DWORD outstandingRequests;
		if (!SetFileBandwidthReservation(handles[i],
			1000,
			200,
			FALSE,
			&transferSize,
			&outstandingRequests))
			std::cout << "Error reserving bandwidth: " << GetLastError() << std::endl;
*/

		if (!SetFileInformationByHandle(handles[i], FileRenameInfo, RenameInfo, infosize))
			std::cout << "Error renaming file: " << GetLastError() << std::endl;
		
        CloseHandle(handles[i]);
	}

    source_file_w.clear();
}


