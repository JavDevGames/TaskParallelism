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

using namespace std;
namespace fs = std::filesystem;

void ParseTestFiles();
void ParseTestFilesWithOpenHandle();
void CreateTestFiles();
void ParseTestFilesConcurrent();
void SetupTestFolders();

int main()
{
    SetupTestFolders();

    CreateTestFiles();
 
    //Single threaded
    ParseTestFiles();

    //With open handles
    ParseTestFilesWithOpenHandle();

    //Single threaded
    //ParseTestFilesConcurrent();
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
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\Output",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles"
	};

	// Directories to add
	vector<string> toAdd = {
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\Output",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles\\move_dir",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\Output\\move_dir",
		"C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles\\move_dir"
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
    string directory = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles\\"; // Directory to save files in

    for (int i = 1; i <= numFiles; i++) {
        stringstream fileName;
        fileName << directory << "file" << i << ".txt"; // Generate file name
        ofstream outFile(fileName.str()); // Open output file
        if (outFile.is_open()) {
            for (int j = 1; j <= wordsPerFile; j++) {
                outFile << "word" << rand() % 100 << " "; // Write a random word
            }
            outFile.close(); // Close output file
        }
        else {
            cout << "Unable to create file " << fileName.str() << endl;
        }
    }
}

__declspec(noinline) void ParseTestFiles()
{
    unordered_map<string, int> wordFreq; // To keep track of word frequency

    std::string directory = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles"; // Directory containing input files
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
        string path = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\Output\\top_words_" + std::to_string(i) + ".txt";

        ofstream outFile(path); // Open output file
        if (outFile.is_open())
        {
            int count = 0;
            for (const auto& wordFreqPair : wordFreqVec)
            {
                outFile << wordFreqPair.first << ": " << wordFreqPair.second << endl; // Write top words to output file
                count++;
                if (count == 10) 
                {
                    // Limit to top 10 words
                    break;
                }
            }
            outFile.close(); // Close output file
            pathsToMove.emplace_back(path);
        }
        else
        {
            cout << "Unable to open output file" << endl;
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

	std::string directory = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles"; // Directory containing input files
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

	std::vector<HANDLE> handles(fileNames.size());
    std::vector<string> toPaths;
	for (size_t i = 0; i < fileNames.size(); ++i)
	{
        string path = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\OutputHandles\\top_words_" + std::to_string(i) + ".txt";

		std::string toPath = path;
		size_t pos = toPath.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
            toPaths.emplace_back(std::move(toPath));
		}

        handles[i] = CreateFileA(path.c_str(), GENERIC_WRITE | GENERIC_READ | DELETE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); // Open output file
		if (handles[i] != INVALID_HANDLE_VALUE)
		{
			int count = 0;
			for (const auto& wordFreqPair : wordFreqVec)
			{
				std::string line = wordFreqPair.first + ": " + std::to_string(wordFreqPair.second) + "\r\n";
				DWORD bytesWritten = 0;
				WriteFile(handles[i], line.c_str(), static_cast<DWORD>(line.size()), &bytesWritten, NULL); // Write top words to output file
				count++;
				if (count == 10)
				{
					// Limit to top 10 words
					break;
				}
			}
		}
		else
		{
			std::cout << "Unable to open output file" << std::endl;
		}
	}

    std::wstring source_file_w;
	for (size_t i = 0; i < handles.size(); ++i)
	{
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

		if (!SetFileInformationByHandle(handles[i], FileRenameInfo, RenameInfo, infosize))
			std::cout << "Error renaming file: " << GetLastError() << std::endl;
		
        CloseHandle(handles[i]);
	}

    source_file_w.clear();
}

__declspec(noinline)  void ParseTestFilesConcurrent()
{
    std::string directory = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\TestFiles"; // Directory containing input files
    std::vector<std::string> fileNames;

    // Get all file paths in the directory
    for (const auto& entry : fs::directory_iterator(directory))
    {
        if (entry.is_regular_file())
        {
            fileNames.push_back(entry.path().string());
        }
    }

    size_t totalThreads = 8;
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

    vector<std::thread> outputThreads;
    outputThreads.resize(totalThreads);

    filePaths.enqueue_bulk(fileNames.begin(), fileNames.size());

    std::atomic<size_t> val = 0;
    for (size_t i = 0; i < totalThreads; ++i)
    {
        outputThreads[i] = std::thread([&filePaths, &val, &chunks](const vector<pair<string, int>> &wordFreqVec) 
        {
            for (size_t i = 0; i < chunks; ++i)
            {
                string path = "C:\\UnitySrc\\JavDev\\TaskParallelism\\TaskParallelism\\OutputConcurrent\\top_words_" + std::to_string(val) + ".txt";
                val++;

                ofstream outFile(path); // Open output file
                if (outFile.is_open())
                {
                    int count = 0;
                    for (const auto& wordFreqPair : wordFreqVec)
                    {
                        outFile << wordFreqPair.first << ": " << wordFreqPair.second << endl; // Write top words to output file
                        count++;
                        if (count == 10)
                        {
                            // Limit to top 10 words
                            break;
                        }
                    }
                    outFile.close(); // Close output file
                }
                else
                {
                    cout << "Unable to open output file" << endl;
                }
            }
        }, std::ref(wordFreqVec));
    }

    for (size_t i = 0; i < outputThreads.size(); ++i)
        outputThreads[i].join();
}
