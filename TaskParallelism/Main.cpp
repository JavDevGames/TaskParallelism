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

void ParseTestFiles();
void ParseTestFilesWithOpenHandle();
void CreateTestFiles();
void ParseTestFilesConcurrent();
void SetupTestFolders();
void ParseTestFilesWithOpenHandleParallel();

int main()
{
	cout << "Setup test folders...";
    SetupTestFolders();
	cout << " Complete" << endl;

	cout << "Create Test Files...";
    CreateTestFiles();
	cout << " Complete" << endl;
 
    //Single threaded
	/*
	cout << "Single Threaded Parse Test Files...";
    ParseTestFiles();
	cout << " Complete" << endl;

    //With open handles
    //ParseTestFilesWithOpenHandle();

    //Multi threaded
	cout << "Multi-threaded Parse Test Files...";
    ParseTestFilesConcurrent();
	cout << " Complete" << endl;
	*/

	// Multi threaded with open handle
	cout << "Multi-threaded Parse Test Files...";
	ParseTestFilesWithOpenHandleParallel();
	cout << " Complete" << endl;
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

__declspec(noinline) void ParseTestFiles()
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

    sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b) { // Sort vector in descending order of word frequency
        return a.second > b.second;
    });

    std::vector<string> pathsToMove;
	for (size_t i = 0; i < fileNames.size(); ++i)
	{
		string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFiles\\top_words_" + std::to_string(i) + ".txt";

		HANDLE handle = CreateFileA(path.c_str(), GENERIC_WRITE | GENERIC_READ | DELETE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); // Open output file
		if (handle != INVALID_HANDLE_VALUE)
		{
			int count = 0;
			for (const auto& wordFreqPair : wordFreqVec)
			{
				std::string line = wordFreqPair.first + ": " + std::to_string(wordFreqPair.second) + "\r\n";
				DWORD bytesWritten = 0;
				WriteFile(handle, line.c_str(), static_cast<DWORD>(line.size()), &bytesWritten, NULL); // Write top words to output file
				count++;
				if (count == 10)
				{
					// Limit to top 10 words
					break;
				}
			}

			pathsToMove.emplace_back(std::move(path));
			CloseHandle(handle);
		}
		else
		{
			std::cout << "Unable to open output file" << std::endl;
		}
	}

	for (size_t i = 0; i < pathsToMove.size(); ++i)
	{
		std::string& fromPath = pathsToMove[i];
		std::string toPath = fromPath;
		size_t pos = toPath.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
		}

		if (!MoveFileExA(fromPath.c_str(), toPath.c_str(), MOVEFILE_COPY_ALLOWED | MOVEFILE_REPLACE_EXISTING))
			std::cout << "Error moving file: " << GetLastError() << std::endl;
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

__declspec(noinline) void ParseTestFilesWithOpenHandleParallel()
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

	struct PathsAndHandles
	{
		string path;
		HANDLE handle;
	};

	moodycamel::ConcurrentQueue <PathsAndHandles> pathsAndHandles;

	for (size_t i = 0; i < fileNames.size(); ++i)
	{
		PathsAndHandles toPath;

		string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel\\top_words_" + std::to_string(i) + ".txt";

		toPath.path = path;

		// And create the new name here
		size_t pos = toPath.path.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.path.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
		}

		std::wstring wpath(path.begin(), path.end());
		DWORD dwDesiredAccess = GENERIC_WRITE | DELETE;
		toPath.handle = CreateFileW(wpath.c_str(),
			dwDesiredAccess,
			0,
			NULL,
			CREATE_ALWAYS,
			NULL,
			NULL); // Open output file

		if (toPath.handle != INVALID_HANDLE_VALUE)
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

			DWORD bytesWritten = 0;
			if(!WriteFile(toPath.handle, contents.c_str(), static_cast<DWORD>(contents.size()), &bytesWritten, NULL)) // Write top words to output file
				std::cout << "Error writing to file: " << GetLastError() << std::endl;

			pathsAndHandles.enqueue(std::move(toPath));
		}
		else
		{
			std::cout << "Unable to open output file: " << GetLastError() << std::endl;
		}
	}

	typedef struct _FILE_RENAME_INFORMATION
	{
		BOOLEAN ReplaceIfExists;
		HANDLE RootDirectory;
		ULONG FileNameLength;
		WCHAR FileName[1];
	} FILE_RENAME_INFORMATION, * PFILE_RENAME_INFORMATION;


	size_t totalThreads = 2;
	vector<std::thread> outputThreads;
	outputThreads.resize(totalThreads);

	size_t chunks = pathsAndHandles.size_approx() / totalThreads;

	std::atomic<size_t> val = 0;
	for (size_t i = 0; i < totalThreads; ++i)
	{
		outputThreads[i] = std::thread([&pathsAndHandles, &val, &chunks]()
		{
			std::wstring source_file_w;

			vector<PathsAndHandles> localPathsAndHandles;
			localPathsAndHandles.resize(chunks);
			pathsAndHandles.try_dequeue_bulk(localPathsAndHandles.begin(), chunks);

			for (size_t i = 0; i < localPathsAndHandles.size(); ++i)
			{
				source_file_w = StringToWideString(localPathsAndHandles[i].path);
				size_t namesize = (wcslen(source_file_w.c_str()) + 1) * sizeof(wchar_t);
				size_t infosize = sizeof(FILE_RENAME_INFORMATION) + namesize;
				FILE_RENAME_INFORMATION* RenameInfo = (FILE_RENAME_INFORMATION*)_alloca(infosize);
				memset(RenameInfo, 0, infosize);
				RenameInfo->ReplaceIfExists = TRUE;
				RenameInfo->RootDirectory = 0;
				RenameInfo->FileNameLength = namesize;
				memcpy(RenameInfo->FileName, source_file_w.c_str(), namesize);

				if (!SetFileInformationByHandle(localPathsAndHandles[i].handle, FileRenameInfo, RenameInfo, infosize))
					std::cout << "Error renaming file: " << GetLastError() << std::endl;

				CloseHandle(localPathsAndHandles[i].handle);

				source_file_w.clear();
			}
		});
	}

	for (size_t i = 0; i < outputThreads.size(); ++i)
		outputThreads[i].join();
}

__declspec(noinline)  void ParseTestFilesConcurrent()
{
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

    size_t totalThreads = 2;
    moodycamel::ConcurrentQueue <string> filePaths;
    size_t chunks = fileNames.size() / totalThreads;

    filePaths.enqueue_bulk(fileNames.begin(), fileNames.size());
    
    vector<unordered_map<string, int>> wordFreq; // To keep track of word frequency
    vector<std::thread> threads;
    threads.resize(totalThreads);

    for(size_t i = 0; i < totalThreads; ++i)
        wordFreq.emplace_back(unordered_map<string, int>());

    for (size_t i = 0; i < totalThreads; ++i)
    {
        threads[i] = std::thread([&filePaths, &chunks](unordered_map<string, int> &wordsPerThread)
        {
            char buf[256];

            vector<string> localFileNames;
            localFileNames.resize(chunks);
            filePaths.try_dequeue_bulk(localFileNames.begin(), chunks);

            for (const auto& fileName : localFileNames)
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
                            ++wordsPerThread[buf];
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
        }, std::ref(wordFreq[i]));
    }

    
    for (size_t i = 0; i < threads.size(); ++i)
        threads[i].join();

    unordered_map<string, int> combinedFrequencies;
    for (size_t i = 0; i < wordFreq.size(); ++i)
    {
        for (auto& curFreq : wordFreq)
        {
            for (auto &[word, count] : curFreq)
                combinedFrequencies[word] += count;
        }
    }

    vector<pair<string, int>> wordFreqVec(combinedFrequencies.begin(), combinedFrequencies.end());

    sort(wordFreqVec.begin(), wordFreqVec.end(), [](const pair<string, int>& a, const pair<string, int>& b) { // Sort vector in descending order of word frequency
        return a.second > b.second;
    });

	for (size_t i = 0; i < fileNames.size(); ++i)
	{
		string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesConcurrent\\top_words_" + std::to_string(i) + ".txt";

		HANDLE handle = CreateFileA(path.c_str(), GENERIC_WRITE | GENERIC_READ | DELETE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); // Open output file
		if (handle != INVALID_HANDLE_VALUE)
		{
			int count = 0;
			for (const auto& wordFreqPair : wordFreqVec)
			{
				std::string line = wordFreqPair.first + ": " + std::to_string(wordFreqPair.second) + "\r\n";
				DWORD bytesWritten = 0;
				WriteFile(handle, line.c_str(), static_cast<DWORD>(line.size()), &bytesWritten, NULL); // Write top words to output file
				count++;
				if (count == 10)
				{
					// Limit to top 10 words
					break;
				}
			}

			filePaths.enqueue(std::move(path));
			CloseHandle(handle);
		}
		else
		{
			std::cout << "Unable to open output file" << std::endl;
		}
	}

    vector<std::thread> outputThreads;
    outputThreads.resize(totalThreads);

    std::atomic<size_t> val = 0;
    for (size_t i = 0; i < totalThreads; ++i)
    {
        outputThreads[i] = std::thread([&filePaths, &val, &chunks](const vector<pair<string, int>> &wordFreqVec) 
        {
			vector<string> localFileNames;
			localFileNames.resize(chunks);
			filePaths.try_dequeue_bulk(localFileNames.begin(), chunks);

            for (size_t i = 0; i < localFileNames.size(); ++i)
            {
				val++;
				string toPath = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesConcurrent\\top_words_" + std::to_string(val) + ".txt";
                
				std::string& fromPath = localFileNames[i];
				
				size_t pos = toPath.find("top_words");
				if (pos != std::string::npos)
				{
					toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
				}

				std::wstring fromPathW = StringToWideString(fromPath);
				std::wstring toPathW = StringToWideString(toPath);

				if (!MoveFileExW(fromPathW.c_str(), toPathW.c_str(), MOVEFILE_REPLACE_EXISTING))
					std::cout << "Error moving file: " << GetLastError() << std::endl;
            }
        }, std::ref(wordFreqVec));
    }

    for (size_t i = 0; i < outputThreads.size(); ++i)
        outputThreads[i].join();

	cout << "Moved " << val << " files" << endl;
}
