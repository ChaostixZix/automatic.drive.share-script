# Certificate Sharing Tool

Automatically share Google Drive certificate folders with participants from a Google Sheets list.
Perfect for educational institutions and training companies managing bulk certificate distribution.

## Quick Start

### 1. Install Dependencies
```bash
npm install
```

### 2. Google Setup
1. Create a service account in Google Cloud Console
2. Enable Google Drive API and Google Sheets API  
3. Download the service account JSON file and save as `service.json`
4. Share your Google Sheet and parent Drive folder with the service account email

### 3. Prepare Google Sheet
Create a sheet with these columns (only first 2 required):
```
Nama | Email
```
The script will auto-add: `FolderId | isShared | isFolderExists | LastLog`

### 4. Create Certificate Folders
In Google Drive, create folders that **exactly match** the names in your "Nama" column:
```
Parent Folder/
├── John Smith/
├── Jane Doe/
└── Bob Johnson/
```

### 5. Run the Tool
```bash
# One-time sharing
node index.js

# Continuous monitoring (auto-shares new participants)  
node monitor.js
```

### 6. Build Executables (Optional)
```bash
# Install pkg globally
npm install -g pkg

# Build for Windows and Mac
npm run build:mac    # Creates certificate-monitor-mac
pkg index.js --targets node18-win-x64 --output certificate-sharing-win.exe
pkg index.js --targets node18-macos-x64 --output certificate-sharing-mac
```

## That's it! 
The tool will automatically:
- Find folders by participant names
- Share folders with participant emails  
- Update the Google Sheet with results
- Skip already shared participants