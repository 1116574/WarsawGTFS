package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/alerts"
	"github.com/MKuranowski/WarsawGTFS/realtime/brigades"
	"github.com/MKuranowski/WarsawGTFS/realtime/gtfs"
	"github.com/MKuranowski/WarsawGTFS/realtime/positions"
	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// Default CLI flags
var (
	// Mode selection
	flagAlerts = flag.Bool(
		"a",
		false,
		"create GTFS-Realtime alerts")

	flagBrigades = flag.Bool(
		"b",
		false,
		"create brigades.json (required for positions)")

	flagPostions = flag.Bool(
		"p",
		false,
		"create GTFS-Realtime vehicle positions")

	// Input options
	flagApikey = flag.String(
		"k",
		"",
		"apikey for api.um.warszawa.pl (for brigades and positions)")

	flagGtfsFile = flag.String(
		"gtfs-file",
		"https://mkuran.pl/gtfs/warsaw.zip",
		"path/URL to static Warsaw GTFS (for alerts and brigades)")

	flagBrigadesFile = flag.String(
		"brigades-file",
		"https://mkuran.pl/gtfs/warsaw/brigades.json",
		"path/URL to file brigades.json (for positions)")

	// Output options
	flagTarget = flag.String(
		"target",
		"data_rt",
		"target folder where to put created GTFS-Realtime files")

	flagJSON = flag.Bool(
		"json",
		false,
		"also save JSON files alongside GTFS-Relatime feeds")

	flagReadable = flag.Bool(
		"readable",
		false,
		"use a human-readable format for GTFS-Realtime target instead of a binary format")

	flagStrict = flag.Bool(
		"strict",
		false,
		"for alerts: any errors when scraping wtp.waw.pl will become fatal,\n"+
			"for brigades: any ignorable data mismatch will become fatal")

	// Loop options
	flagLoop = flag.Duration(
		"loop",
		time.Duration(0),
		"alerts/positions: instead of running once and exiting, "+
			"update the output file every given duration\n"+
			"brigades: if non-zero, update the brigades file every time -gtfs-file changes")

	flagDataCheck = flag.Duration(
		"checkdata",
		time.Duration(30*60*1_000_000_000), // 30 minutes in ns
		"alerts/brigades: how often check if the -gtfs-file has changed,\n"+
			"positions: how often check if the -brigades-file has changed")
)

// Other global objects
var gtfsFile *gtfs.Gtfs
var client = &http.Client{}

// checkModes ensures exactly one of flagAlerts, flagBrigades or flagPositions is set
func checkModes() error {
	var modeCount uint8

	if *flagAlerts {
		modeCount++
	}
	if *flagBrigades {
		modeCount++
	}
	if *flagPostions {
		modeCount++
	}

	if modeCount != 1 {
		return errors.New("exactly one of the -a, -b or -p flags has to be provided")
	}
	return nil
}

// loadGtfs creates a gtfs file from the provided argument and loads required data structures
func loadGtfs() (err error) {
	// retrieve the GTFS
	log.Println("Retrieving provided GTFS")
	if strings.HasPrefix(*flagGtfsFile, "http://") || strings.HasPrefix(*flagGtfsFile, "https://") {
		gtfsFile, err = gtfs.NewGtfsFromURL(*flagGtfsFile, client)
	} else {
		gtfsFile, err = gtfs.NewGtfsFromFile(*flagGtfsFile)
	}

	if err != nil {
		return
	}

	// Load data
	if *flagAlerts {
		log.Println("Loading routes.txt")
		if routesFile := gtfsFile.GetZipFileByName("routes.txt"); routesFile != nil {
			err = gtfsFile.LoadRoutes(routesFile)
		} else {
			err = errors.New("no file routes.txt in the GTFS")
		}
	} else {
		log.Println("Loading data from the GTFS")
		err = gtfsFile.LoadAll()
	}

	return
}

// mainAlerts prepares options for creating alerts and then creates them
func mainAlerts() (err error) {
	// Create options struct
	opts := alerts.Options{
		GtfsRtTarget:    path.Join(*flagTarget, "alerts.pb"),
		HumanReadable:   *flagReadable,
		ThrowLinkErrors: *flagStrict,
	}
	if *flagJSON {
		opts.JSONTarget = path.Join(*flagTarget, "alerts.json")
	}

	// Make alerts
	log.Println("Creating Alerts feed")
	err = alerts.Make(client, gtfsFile.Routes, opts)
	return
}

// loopAlerts prepares options for launching laerts in a loop mode and then launches the loop mode
func loopAlerts() error {
	// Create options struct
	opts := alerts.Options{
		GtfsRtTarget:    path.Join(*flagTarget, "alerts.pb"),
		HumanReadable:   *flagReadable,
		ThrowLinkErrors: *flagStrict,
	}
	if *flagJSON {
		opts.JSONTarget = path.Join(*flagTarget, "alerts.json")
	}

	// Create the resource with data
	var gtfsResource util.Resource
	if strings.HasPrefix(*flagGtfsFile, "http://") || strings.HasPrefix(*flagGtfsFile, "https://") {
		gtfsResource = &util.ResourceHTTP{
			Client: client, URL: *flagGtfsFile, Peroid: *flagDataCheck,
		}
	} else {
		gtfsResource = &util.ResourceLocal{Path: *flagGtfsFile, Peroid: *flagDataCheck}
	}

	// Call alerts.Loop
	return alerts.Loop(client, gtfsResource, *flagLoop, opts)
}

// mainBrigades prepares options for creating brigades and then creates them
func mainBrigades() (err error) {
	if *flagApikey == "" {
		return errors.New("Key for api.um.warszawa.pl needs to be provided")
	}

	// Create options struct
	opts := brigades.Options{
		JSONTarget:     path.Join(*flagTarget, "brigades.json"),
		Apikey:         *flagApikey,
		ThrowAPIErrors: *flagStrict,
	}
	// Make alerts
	log.Println("Creating brigades.json")
	err = brigades.Main(client, gtfsFile, opts)
	return
}

// mainPositions parses options for creating vehicle positions and then creates them
func mainPositions() (err error) {
	if *flagApikey == "" {
		return errors.New("Key for api.um.warszawa.pl needs to be provided")
	}

	// Create options struct
	opts := positions.Options{
		GtfsRtTarget:  path.Join(*flagTarget, "positions.pb"),
		HumanReadable: *flagReadable,
		Apikey:        *flagApikey,
		Brigades:      *flagBrigadesFile,
	}
	if *flagJSON {
		opts.JSONTarget = path.Join(*flagTarget, "positions.json")
	}

	// Make alerts
	log.Println("Creating brigades.json")
	err = positions.Main(client, opts)
	return
}

// Main functionality
func main() {
	var err error

	// Parse CL flags
	flag.Parse()

	// Check excluding flags
	loopMode := *flagLoop > 0
	err = checkModes()
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Load gtfs
	if !*flagPostions && !loopMode {
		err = loadGtfs()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	// Launch specified mode
	switch {
	case *flagAlerts && loopMode:
		err = loopAlerts()
	case *flagAlerts:
		err = mainAlerts()
	case loopMode:
		err = errors.New("loop mode is only available for alerts")
	case *flagBrigades:
		err = mainBrigades()
	case *flagPostions:
		err = mainPositions()
	}

	if err != nil {
		log.Fatalln(err.Error())
	}
}
