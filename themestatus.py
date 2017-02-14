#!/usr/bin/env python

import argparse, sys, os, codecs, json
import parsecpp

print ('Checking theme progress / completeness...')

sourceDir = None

def main():
    parser = argparse.ArgumentParser(description='Checking MythTV theme progress / completeness')
    parser.add_argument('-s', '--source-dir', dest='sourceDir', help='directory of MythTV sources to parse')
    parser.add_argument('-t', '--theme-dir', dest='themeDir', help='directory of a MythTV theme to parse')
    parser.add_argument('--source-file', dest='sourceFile', help='file name of cached MythTV source parsing results')
    parser.add_argument('--theme-file', dest='themeFile', help='file name of cached MythTV theme parsing results')
    parser.add_argument('-d', '--dump', dest='dumpResults', action='store_true', help='dump parsing results to json encoded cache file')
    parser.add_argument('-v', '--verbose', dest='verbose', nargs='?', const='all', help='show verbose output, specify \'source\', \'theme\',  \'missing\' or \'all\' to constrain output to print corresponding theme windows lists')
    args = parser.parse_args()
    if not args.verbose:
        args.verbose =('')
    elif 'all' in args.verbose:
        args.verbose = ('source', 'theme', 'missing')

    if not args.sourceDir and not args.sourceFile:
        print("Please specify a source directory or source cache file")
        sys.exit(1)
    if not args.themeDir and not args.themeFile:
        print("Please specify a theme directory or theme cache file")
        sys.exit(2)
    if args.sourceDir and not os.path.exists(args.sourceDir):
        print("Specified source directory does not exists: " + args.sourceDir)
        sys.exit(3)
    if args.themeDir and not os.path.exists(args.themeDir):
        print("Specified theme directory does not exists: " + args.themeDir)
        sys.exit(4)
    if args.sourceFile and not os.path.exists(args.sourceFile):
        print("Specified source cache file does not exists: " + args.sourceFile)
        sys.exit(5)
    if args.themeFile and not os.path.exists(args.themeFile):
        print("Specified theme chache file does not exists: " + args.themeFile)
        sys.exit(6)

    if args.sourceDir:
        print("\nLooking for theme windows referenced in source files in " + args.sourceDir)
        sourceWindows = iterateDir(args.sourceDir, parseSourceFile)
        if args.dumpResults:
            dumpFile = os.path.basename(args.sourceDir.rstrip(os.sep)) + '.txt'
            print("\nExporting theme window found in source files to " + dumpFile)
            exportMapping(sourceWindows, dumpFile)
    elif args.sourceFile:
        print("\nImporting theme windows from file " + args.sourceFile)
        sourceWindows = importMapping(args.sourceFile)

    sourceWindowCount = countWindows(sourceWindows)
    if 'source' in args.verbose:
        printMapping(sourceWindows)
    print("\tFound " + str(sourceWindowCount) + " theme windows in source files");

    if args.themeDir:
        print("\nLooking for theme windows referenced in theme files in " + args.themeDir)
        themeWindows = iterateDir(args.themeDir, parseThemeFile)
        if args.dumpResults:
            dumpFile = os.path.basename(args.themeDir.rstrip(os.sep)) + '.txt'
            print("\nExporting theme window found in theme files to " + dumpFile)
            exportMapping(themeWindows, dumpFile)
    elif args.themeFile:
        print("\nImporting theme windows from file " + args.themeFile)
        themeWindows = importMapping(args.themeFile)

    themeWindowCount = countWindows(themeWindows)
    if 'theme' in args.verbose:
        printMapping(themeWindows)
    print("\tFound " + str(themeWindowCount) + " theme windows in theme files");

    if len(sourceWindows) and len(themeWindows):
        print("\nLooking for missing theme windows in theme files")
        missingWindows = compareWindows(sourceWindows, themeWindows)
        missingWindowCount = countWindows(missingWindows)
        if 'missing' in args.verbose:
            printMapping(missingWindows)
        print("\tFound " + str(missingWindowCount) + " missing theme windows");

        print("\nLooking for obsolete theme windows in theme files")
        obsoleteWindows = compareWindows(themeWindows, sourceWindows)
        obsoleteWindowCount = countWindows(obsoleteWindows)
        if 'missing' in args.verbose:
            printMapping(obsoleteWindows)
        print("\tFound " + str(obsoleteWindowCount) + " obsolete theme windows");

        doneWindowCount = sourceWindowCount-missingWindowCount;
        print("\nTheme is {:.1%} complete ({} of {} theme windows are done, {} windows are missing)".format(doneWindowCount/sourceWindowCount, doneWindowCount, sourceWindowCount, missingWindowCount))

    exit(0)

# print mapping of theme file -> theme windows
def printMapping(windows):
    for uiFile, uiWindows in sorted(windows.items()):
        printWindows(uiFile, uiWindows)

def printWindows(uiFile, uiWindows):
    print("\t" + uiFile + " (" + str(len(uiWindows)) + ")")
    for w in sorted(uiWindows):
        print("\t\t" + w)

def countWindows(windows):
    windowCount = 0
    for uiFile, uiWindows in windows.items():
        windowCount += len(uiWindows)
    return windowCount

# write theme file -> theme window mapping to json encoded file
def exportMapping(windows, filename):
    jsonWindows = {}
    for uiFile, uiWindows in sorted(windows.items()):
        jsonWindows[uiFile] = list(uiWindows)
    with open(filename, 'w') as outfile:
        json.dump(jsonWindows, outfile)

# read theme file -> theme window mapping from json encoded file
def importMapping(filename):
    jsonWindows = None
    with open(filename, 'r') as infile:
        jsonWindows = json.load(infile)
    windows = {}
    for uiFile, uiWindows in sorted(jsonWindows.items()):
        windows[uiFile] = set(uiWindows)
    return windows

# compare theme windows parsed from source code with found theme windows
# parsed from theme files, show missing theme windows
def compareWindows(defWindows, curWindows):
    diffWindows = {}
    # iterate theme windows as defines in source code and try to match
    # them to the theme windows found in theme files
    for uiFile, uiWindows in defWindows.items():
        if not uiFile in curWindows:
            # this theme file was not found in the parsed theme
            diffWindows[uiFile] = uiWindows
        else:
            # determine theme windows missing in parsed theme
            diff = uiWindows - curWindows[uiFile]
            if diff:
                diffWindows[uiFile] = diff
    return diffWindows

# walk directories and parse files
def iterateDir(dirName, parse):
    results = {}
    for root, dirs, files in os.walk(dirName):
        for name in files:
            r = parse(dirName, os.path.join(root, name))
            if (r):
                for key,value in r.items():
                    if key in results:
                        # theme file already found, append theme windows
                        results[key] |= value
                    else:
                        # theme file first encountered, assign theme windows
                        results[key] = value
    return results

# parses a theme file, returns a dict{theme file: {theme windows}, ...}
def parseThemeFile(themeDir, fileName):
    if not fileName.endswith(".xml"):
        return {}
    themeFile = os.path.basename(fileName)
    #print(themeFile)
    themeWindows = set()
    with openTextFile(fileName) as f:
        for l in f:
            if "window name" in l:
                fields = l.split('"')
                if len(fields) > 2:
                    themeWindows.add(fields[1])
    if themeWindows:
        return {themeFile : themeWindows}
    else:
        return {}

# add new window to themeFile
def addWindow(windowDict, themeFile, themeWindow):
    if themeFile in windowDict:
        windowDict[themeFile].add(themeWindow)
    else:
        windowDict[themeFile] = {themeWindow}

# parses a source file, returns a dict{theme file: {theme windows}, ...}
def parseSourceFile(sourceDir, fileName):
    if not fileName.endswith(".cpp"):
        return {}
    if fileName.endswith("xmlparsebase.cpp"):
        return {}
    #print(os.path.basename(fileName))
    results = {}
    with openTextFile(fileName) as f:
        lineNumber = 0;
        for l in f:
            # keep track of the line number used to display any errors
            lineNumber += 1
            if "LoadWindowFromXML" in l:
                fields = l.split('"')
                if len(fields) > 3:
                    themeFile = fields[1]
                    themeWindow = fields[3]
                    addWindow(results, themeFile, themeWindow)
                    continue
                else:
                    args = parseArguments(sourceDir, fileName, "LoadWindowFromXML", lineNumber)
                    if args and len(args) >= 2 and args[0] and args[1]:
                        #print(args)
                        for w in args[1]:
                            addWindow(results, args[0][0], w)
                        continue
            elif "CopyWindowFromBase" in l:
                fields = l.split('"')
                if len(fields) > 2:
                    themeFile = "base.xml"
                    themeWindow = fields[1]
                    addWindow(results, themeFile, themeWindow)
                    continue
                else:
                    args = parseArguments(sourceDir, fileName, "CopyWindowFromBase", lineNumber)
                    if args and len(args) >= 1 and args[0]:
                        #print(args)
                        for w in args[0]:
                            addWindow(results, "base.xml", w)
                        continue
            elif "CreateEditChild" in l:
                fields = l.split('"')
                if len(fields) > 3:
                    themeFile = fields[1]
                    themeWindow = fields[3]
                    addWindow(results, themeFile, themeWindow)
                    continue
                else:
                    args = parseArguments(sourceDir, fileName, "CreateEditChild", lineNumber)
                    if args and len(args) >= 2 and args[0] and args[1]:
                        #print(args)
                        for w in args[1]:
                            addWindow(results, args[0][0], w)
                        continue
            elif "MythOSDWindow" in l:
                fields = l.split('"')
                if len(fields) > 2:
                    themeFile = "osd.xml"
                    themeWindow = fields[1]
                    addWindow(results, themeFile, themeWindow)
                    continue
                else:
                    args = parseArguments(sourceDir, fileName, "MythOSDWindow", lineNumber)
                    if args and len(args) >= 1 and args[1]:
                        #print(args)
                        for w in args[1]:
                            addWindow(results, "osd.xml", w)
                        continue
            else:
                continue
            # parsing failed somehow, output filename and line number
            sys.stderr.write(os.path.basename(fileName) + " line " + str(lineNumber) + ":")
            sys.stderr.write(l.strip())
            sys.stderr.write('\n')
    return results

def parseArguments(sourceDir, fileName, funcName, lineNr):
    dumpFile = parsecpp.createDump(sourceDir, fileName)
    if not dumpFile:
        return 1
    funcArgs = parsecpp.findFunctionArgs(dumpFile, funcName, str(lineNr))
    os.remove(dumpFile)
    return funcArgs

# open text file, trying different encodings
def openTextFile(fileName):
    encodings = ['utf-8', 'latin1', 'ascii']
    for e in encodings:
        try:
            f = codecs.open(fileName, 'r', encoding=e)
            f.readlines()
            f.seek(0)
        except UnicodeDecodeError:
            #print('got unicode error with %s , trying different encoding' % e)
            continue
        else:
            # print('opening the file with encoding:  %s ' % e)
            break
    return f

if __name__ == "__main__":
    main()
