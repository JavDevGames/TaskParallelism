#include "OpenHandleConcurrentFileProcessor.h"

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
		string relativePath;
		HANDLE handle;
	};

	moodycamel::ConcurrentQueue <PathsAndHandles> pathsAndHandles;

	HANDLE moveDirHandle = CreateFileW(L"C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel\\move_dir", 
		FILE_ADD_FILE,
		FILE_SHARE_READ | FILE_SHARE_WRITE,
		NULL,
		OPEN_EXISTING,
		FILE_FLAG_BACKUP_SEMANTICS,
		NULL);

	if (moveDirHandle == INVALID_HANDLE_VALUE)
	{
		std::cout << "Error opening move directory: " << GetLastError() << std::endl;
	}

	for (size_t i = 0; i < fileNames.size(); ++i)
	{
		PathsAndHandles toPath;

		string fileName = "top_words_" + std::to_string(i) + ".txt";
		string path = "C:\\Projects\\JavDev\\TaskParallelism\\TaskParallelism\\ParseTestFilesWithOpenHandleParallel\\" + fileName;

		toPath.path = path;
		toPath.relativePath = fileName;

		// And create the new name here
		size_t pos = toPath.path.find("top_words");
		if (pos != std::string::npos)
		{
			toPath.path.replace(pos, std::string("top_words").length(), "move_dir\\top_words");
		}

		std::wstring wpath(path.begin(), path.end());
		DWORD dwDesiredAccess = FILE_GENERIC_WRITE  | FILE_WRITE_DATA| DELETE;
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

			if (!LockFile(toPath.handle, 0, 0, 0, 0))
				std::cout << "Failed to lock file: " << GetLastError() << std::endl;

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
		outputThreads[i] = std::thread([&pathsAndHandles, &val, &chunks, &moveDirHandle]()
		{
			std::wstring source_file_w;

			vector<PathsAndHandles> localPathsAndHandles;
			localPathsAndHandles.resize(chunks);
			pathsAndHandles.try_dequeue_bulk(localPathsAndHandles.begin(), chunks);

			for (size_t i = 0; i < localPathsAndHandles.size(); ++i)
			{
				source_file_w = StringToWideString(localPathsAndHandles[i].path);
				size_t namesize = (wcslen(source_file_w.c_str())+1) * sizeof(wchar_t);
				size_t infosize = sizeof(FILE_RENAME_INFORMATION) + namesize;
				FILE_RENAME_INFORMATION* RenameInfo = (FILE_RENAME_INFORMATION*)_alloca(infosize);
				memset(RenameInfo, 0, infosize);
				RenameInfo->RootDirectory = NULL;
				RenameInfo->ReplaceIfExists = TRUE;
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

	CloseHandle(moveDirHandle);
}

