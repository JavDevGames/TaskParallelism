#include "OpenHandleFileProcessor.h"
#include <filesystem>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string.h>
#include <Windows.h>
#include "../concurrentqueue.h"
#include "../Utils.h"

using namespace std;
namespace fs = std::filesystem;

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

	size_t sectorSize = Utils::GetSectorSize();

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
			if(!WriteFile(handles[i], pMemory, (DWORD) sectorSize, &bytesWritten, NULL)) // Write top words to output file
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

        source_file_w = Utils::StringToWideString(toPaths[i]);

		ULONG namesize = (ULONG)((wcslen(source_file_w.c_str()) + 1) * sizeof(wchar_t));
		ULONG infosize = (ULONG) (sizeof(FILE_RENAME_INFORMATION) + namesize);
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
