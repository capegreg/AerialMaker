/***
	AerialMaker
	Created by: GBologna
	Created On: 1/15/2020
	Git: repos\AerialMaker
	Usage: params
		_rowsToFetch = how many to run
		_offset = zero for all
		run from aerialServiceRun.js or command: >node.exe aerialmaker.js
	Note: ArcGIS requires a delay before reading URL with https.get
	Last updated: 2/19/2020
 */
// const vscode = require('vscode');
// const session = vscode.debug.activeDebugSession;

const isDebugMode = process.env.DEBUG_MODE;

const path = require('path');
// for image name
const uuidv1 = require('uuid/v1');

// for writing image to disk
const fs = require('fs');

// https will save esri aerial to png
const https = require('https');
const url = require("url");
const { promisify } = require('util');
const appendFile = promisify(fs.appendFileSync);

//
const singleLineString = require('./utils.js');

// Image packages
// const sharprz = require('./image-resize-sharp'); // not used here, use for creating thumbnails
// const sharp = require('sharp'); // not used here, but package needs to be updated. https://www.npmjs.com/package/sharp

// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

// ArcGIS packages
require("cross-fetch/polyfill");
require("isomorphic-form-data");
const { request } = require("@esri/arcgis-rest-request");

// for emailing logs
const nodemailer = require("nodemailer");

const _jobDate = new Date();
const timestamp = Date.now();

// used for job start\end
let tsStart = new Date();

// YYYY-mm-dd_HH-MM-SS
const d = [_jobDate.getFullYear(), _jobDate.getMonth() + 1, _jobDate.getDate()];
let _logFileDate = d.map((d) => d.toString().padStart(2, '0')).join('-');

const t = [_jobDate.getHours(), _jobDate.getMinutes(), _jobDate.getSeconds()];
_logFileDate += '_' + t.map((t) => t.toString().padStart(2, '0')).join('-');

const _logFileDateTime = new Date(Math.floor(timestamp));

/**
	Return aerial map year
 */
const getMapYear = () => new Promise((resolve, reject) => {
	
	request(_map_meta_url)
	.then((response) => {

		var _yr = '';

		const regex = /\d{4}/g;
		while ((m = regex.exec(response.serviceDescription)) !== null) {
				// This is necessary to avoid infinite loops with zero-width matches
				if (m.index === regex.lastIndex) {
						regex.lastIndex++;
				}
				// The result can be accessed through the `m`-variable.
				m.forEach((match, groupIndex) => {
						//console.log(`Found match, group ${groupIndex}: ${match}`);
						_yr = match; // by organization convention, assume this is a year
				});
		}

		resolve(_yr);
	})
	.catch( error => {
			logJobError(`getMapYear(): ${error}`);
			reject(error);
	});
});

/***
	Return aerial extent
 */
async function getAerialExtents(parid) {		
	
	let url = `https://gis.xxxpao.com/arcgis/rest/services/Website/WebLayers/MapServer/0/query?where=PARID%3D%27${parid}%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=PARID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=true&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=json`;

  let extentsPromise = new Promise((resolve, reject) => {    
		request(url)
		.then((response) => {
			if(isNaN(response.extent.xmax) || isNaN(response.extent.xmin) || isNaN(response.extent.ymax) | isNaN(response.extent.ymin) ) {
				reject(false);
			} else {
				//TODO: expand extent by 1.5
				resolve(response);
			}
		})
		.catch(error => {
			logJobError(`extentsPromise() on parid: ${parid}: ${error}`);			
		});			
  });
	let result = await extentsPromise;
	return result;
}

async function exportWorkflow () {

	let promise = new Promise((resolve, reject) => {
		
		imageExportWorkflow()
		.then(() => {
			resolve(true);
		});		
		
	});
	let result = await promise;
	return result;
}

/**
 */
const imageExportWorkflow = async () => {

		for await (let parid of _parcelsList) {
			
			if(parid !== undefined) {

				var _filename = [parid, _mapYear, 'aerial' ];
				IdocxDocument.FileName = _filename.join('_') + '.png'; 

				let response = await getAerialExtents(parid)
				.catch(response => {
					// rejected parids will be skipped
					if(!response)
						_parcelsGisProblem.push(parid);
				});
				
				if(response) { 
					await exportMapTask(parid, response.extent)
					.then(response => {
						try {
							var q = url.parse(response, true);
							var has_png = path.extname(q.path);
							if(has_png.toUpperCase() === ".PNG") {
								return response;
							} else {								
								return false;
							}
						} catch (error) {
							logJobError(`exportMapTask() on parid: ${parid}: ${error}`);
							return "skip";
						}
					})			
					.then(response => {
						var imgUrl = response;
						return saveAerialImage(imgUrl)
					})
					.then(response => {
						if(response != false) {
							return setFileAttributes(response)
						}
					})
					.then(response => {
						if(response === true) {
							return insertIdocs(parid)
						}
					})
					.catch(error => {
						logJobError(`getAerialExtents->exportMapTask() on parid: ${parid}: ${error}`);
					});
				}
			}
		}
}

/**
 */
const setFileAttributes = (file) => new Promise((resolve, reject) => {
	fs.stat(file, (error, stats) => {
		if (error) {
			logJobError(`setFileAttributes() on file: ${file}: ${error}`);
			reject(error)
		} else {
			IdocxDocument.FileSizeInBytes = stats.size;
			resolve(true)
		}
	});
})

/**
 */
async function exportMapTask(parid, extent) {

	let promise = new Promise((resolve, reject) => { 

		try {
														
			var parcelLines = 1;
			var xmin=ymin=xmax=ymax=wkid=latestWkid=urlA="";

			wkid =  extent.spatialReference.wkid;
			latestWkid = extent.spatialReference.latestWkid;
			var requestUrl = _printServiceUrl + "/execute?";

			xmax = extent.xmax;
			xmin = extent.xmin;
			ymax = extent.ymax;
			ymin = extent.ymin;

			// expand extent
      var extentWidth = extent.xmax - extent.xmin;
      var extentHeight = extent.ymax - extent.ymin;
			
			xmin = extent.xmin - 0.25 * extentWidth;
			ymin = extent.ymin - 0.25 * extentHeight;
			xmax = extent.xmax + 0.25 * extentWidth;
			ymax = extent.ymax + 0.25 * extentHeight;

			var symbol = {
					"type": "simple",
					"symbol": {
						"type": "esriSFS",
						"style": "esriSFSNull",
						"outline": {
							"type": "esriSLS",
							"style": "esriSLSSolid",
							"color": [66, 244, 206],
							"width": 3
						}
					}
			}; // end symbol
			var drawingInfo = {
				"renderer": symbol
			}; // end drawingInfo
			var webParcelLinesLayers = {
				"opacity": 1,
				"minScale": 0,
				"maxScale": 0,
				"url": _mapLayerAndLabelsUrl
			};
			var webParcelLinesLayers2 = {
				"opacity": 1,
				"minScale": 0,
				"maxScale": 0,
				"url": _mapLayerAndLabelsUrl,
				"layers": [
					{
						"id": 263,
						"layerDefinition": {
							"source": {
								"type": "mapLayer",
								"mapLayerId": 263
							}
						}
					}
				],					
				"visibleLayers": [
					263
				]
			};
			var webParcelLinesLayers3 = {
				"opacity": 1,
				"minScale": 0,
				"maxScale": 0,
				"url": _mapLayerAndLabelsUrl,
				"layers": [
					{
						"id": parcelLines,
						"layerDefinition": {
							"drawingInfo": drawingInfo,
							"source": {
								"type": "mapLayer",
								"mapLayerId": parcelLines
							},
							"definitionExpression": "PARID='"+parid+"'"
						}
					}
				]
			};
			var operationalLayers = [{
				"opacity": 1,
				"minScale": 0,
				"maxScale": 0,
				"url": _latestYearAerialUrl
			}]; // end operationalLayers
			operationalLayers.push(webParcelLinesLayers);
			operationalLayers.push(webParcelLinesLayers3);

			var json = {
				"mapOptions": {
					"showAttribution": false, 						
					"extent": {
						"xmin": xmin, 
						"ymin": ymin, 
						"xmax": xmax, 
						"ymax": ymax,							
						"spatialReference": { 
							"wkid": wkid
						}
					}, 
					"spatialReference": { 
						"wkid": wkid 
					},
					"rotation" : 0
				},
				"operationalLayers": operationalLayers,					
				"exportOptions": { 
					"outputSize": [400, 400], 
					"dpi": 96 // if layout = 'map-only', w and h need to be modified proportional to the dpi change. Default = 96.
				}
			}; // end json

			var Web_Map_as_JSON = JSON.stringify(json);
					
			var params = { 
				httpMethod: 'POST',
				Web_Map_as_JSON: Web_Map_as_JSON,
				Format: 'PNG8',
				Layout_Template: 'MAP_ONLY',
				f: 'json'
			};

			request(requestUrl, {
				params: params
			})
			.then(response => {
				if(response.results[0].value.url.length>0) {
					console.log(`\nParid: ${parid}\nArcGIS Aerial: ${response.results[0].value.url}`);
					resolve(response.results[0].value.url);
				}
			})
			.catch(error => {
				logJobError(`exportMapTask() on parid: ${parid}: ${error}`);
				reject(false);
			});
		} catch (error) {
			logJobError(`${error}`);
		}	

  });

	let result = await promise;
	return result;
}

// Node.js provides also a sync method, which blocks the thread until the file stats are ready
function createFile() {

		var uuid = uuidv1();
		IdocxDocument.FileSystemId = uuid;
		var folder = path.join(_exportfolder, uuid.substr(0, 2));

		if (!fs.existsSync(folder)) {
			try {
				fs.mkdirSync(folder);
			} catch (error) {				
				logJobError(`createFile(): ${error}`);
				return false;
			}			
		}
		var p = path.join(folder, uuid + '.png');
		try {
			fs.accessSync(folder);
			return p;
		} catch (error) {
			logJobError(`${error}`);
			return false;
		}
}

async function saveUrlAerial(url) {

	let imagePromise = new Promise((resolve, reject) => {

		var request = https.get(url, function(response) {

			const { statusCode } = response;

			if(statusCode === 200) {

				var fn = createFile();
				let file = fs.createWriteStream(fn, {
						flags: 'w' // default
				});

				response.pipe(file);
				file.on('finish', function() {
					if(!(file.bytesWritten>594)) {
						logJobError(`saveUrlAerial() on url: ${url}\nbytesWritten: ${file.bytesWritten}`);
					// } else {
					// 	console.log(`bytesWritten: ${file.bytesWritten}`);
					}
					file.close();
					resolve(file.path)
				});
				file.on('error', error => {
					file.close();					
					logJobError(`saveAerialImage() on url: ${url}: ${error}`);
					reject(false)
				});

			} else {
				logJobError(`saveUrlAerial() on url: ${url}\nstatusCode: ${statusCode}`);
				reject(false)
			}

		});	
	});

	let result = await imagePromise;
	return result;

}

/**
 */	
const saveAerialImage = (url) => new Promise((resolve, reject) => {

	// need delay before getting ArcGIS URL
	setTimeout(() =>  
		saveUrlAerial(url)
		.then((response) => {
			resolve(response);
		})
		.catch(error => {
				logJobError(`saveAerialImage() on url: ${url}: ${error}`);	
				reject(false);		
		})
	, 2000);

});

/**
 */
const setDocumentType = () => new Promise((resolve, reject) => {

	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {

		const result = connection.execute(
			`SELECT dt.ID, dt.version, dt.document_system, dtf.FIELD_TYPE
			FROM idocs.IDOCX_DOCUMENT_TYPE dt 
  			join idocs.IDOCX_DOCUMENT_TYPE_FIELD dtf on dtf.document_type=dt.id 
  			join idocs.IDOCX_FIELD_TYPE ft on ft.id=dtf.field_type 
			WHERE upper(ft.name) = :name`,
			['PHOTO CATEGORY'],
			{ 
				maxRows: 1,
				outFormat: oracledb.OUT_FORMAT_OBJECT,
				extendedMetaData: true 
			}
		)
		.then(result => {
			IdocxDocumentType.Id = result.rows[0]['ID'];
			IdocxDocumentType.Version = result.rows[0]['VERSION'];
			IdocxDocumentType.DocumentSystem = result.rows[0]['DOCUMENT_SYSTEM'];
			IdocxDocumentType.FieldType = result.rows[0]['FIELD_TYPE'];
			connection.close();
			resolve(true);
		})
		.catch(error => {
			connection.close();			
			logJobError(`setDocumentType(): ${error}`);
			reject(false);
		});
	})
	.catch(error => {
		logJobError(`setDocumentType(): ${error}`);
	});
});

/**
 */
const doWorkflow = () => new Promise((resolve, reject) => {

	getParcelsToRun()
 	.then(response => {
		if(response > 0) {
			if(_parcelsList.length>0) {
				exportWorkflow()
				.then(response => {
					resolve(response);
				})
				.catch(error => {
					logJobError(`exportWorkflow(): ${error}`);
					reject(false);
				});
			}
		}
	})
	.catch(error => {		
		logJobError(`getParcelsToRun(): ${error}`);
		reject(false);
	});
});

/**
 */
function getAttachments() {
	// attachment
	let a = [];
	try {
		if (fs.existsSync(_aerialmaker_export_log))
			a.push({filename: _export_logs, path: _aerialmaker_export_log, contentType: 'text/plain', contentDisposition: 'attachment'});
		if (fs.existsSync(_aerialmaker_error_log))
			a.push({filename: _error_logs, path: _aerialmaker_error_log, contentType: 'text/plain', contentDisposition: 'attachment'});
		if (fs.existsSync(_missed_gis_hdr_rpt))
			a.push({filename: _missed_gis_hdr_rpt, path: _missed_gis_hdr_rpt, contentType: 'text/plain', contentDisposition: 'attachment'});			
	} catch (error) {
		logJobError(`createAttachments(): ${error}`);
	}
	return a;
}

/**
 */
function found() {
	return (_parcelsList.length === 0 ? `No parcels were` : `${_parcelsList.length} ${(_parcelsList.length > 1 ? 'parcels were' : 'parcel was')}`) + ' found for aerial image export.\n';
}

/**
 */
 function missed() {
	 return (_parcelsGisProblem.length === 0 ? 'There were no GIS problem parcels' : `${_parcelsGisProblem.length} ${(_parcelsGisProblem.length > 1 ? 'GIS problem parcels were' : 'GIS problem parcel was')}`)	+ ` logged.\n`;	
 }

/**
 */
function success() {
	var s = '';
	if(_parcelsList.length - _parcelsGisProblem.length > 0) {
		s = `${_parcelsList.length - _parcelsGisProblem.length} aerial image${(_parcelsList.length - _parcelsGisProblem.length) > 1 ? 's' : ''} ${(_parcelsList.length - _parcelsGisProblem.length) > 1 ? 'were' : 'was'} successfully exported to ${_exportfolder}`;
	} else {
		s = `No images were exported.`;
	}
	return s;
}

/**
 */
const mailLogs = async () => {

	let title = `Aerial Maker results for ${_logFileDateTime}\nEnvironment: ${IMAGE_ENV}\n`;
	let body = `<h3>Aerial Maker results for ${_logFileDateTime}</h3>
		<div><ul>
		<li>${found()}</li>
		<li>${success()}</li>
		<li>${missed()}</li>
		${writeErrorSummary(true)}
		${jobLogReport(true)}
		${jobLogReportForMapping(true)}
		${jobStatusReport(true)}</ul></div>`;
	
	let html = htmltpl(title, body);

	var from = '"';
	var to = ""; // list of receivers WebsiteIssue
	var subject = "Aerial Maker daily report";
	if(IMAGE_ENV === "DEV")
		subject += `\xa0--\xa0Environment\xa0${IMAGE_ENV}`;

	var attachments = ''; //getAttachments();

	// send mail to distribution groups
	sendthemail(html, attachments, from, to, subject)
	
	if(_parcelsGisProblem.length>0) {
		title = `Aerial Maker GIS Problem Parcels for ${_logFileDateTime}`;
		body = `<h3>Aerial Maker GIS Problem Parcels for ${_logFileDateTime}</h3>
		<div><ul>
		<li>${missed()}</li>
		${jobLogReportForMapping(true)}</ul></div>`;
	
		html = htmltpl(title, body);

		from = '';
	 	to = ""; 
		subject = "Aerial Maker GIS Problem Parcels daily report";
		attachments = '';
		//sendthemail(html, attachments, from, to, subject)
	}
	return true;
}

/**
 * (node:10584) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
 */
const sendthemail = async (html, attachments, from, to, subject) => {

	// async..await is not allowed in global scope, must use a wrapper

	// create reusable transporter object using the default SMTP transport
	let transporter = nodemailer.createTransport({
		host: "",
		port: 25,
		secure: false, // true for 465, false for other ports
		//requireTLS: true,
		// auth: {
		//   user: "",
		//   pass: ""
		// },
		tls:{
    	// do not fail on invalid certs
      rejectUnauthorized: false
		  // ciphers:'SSLv3'
		}
	});

	let check = await transporter.verify(function(error, success) {
		if (error) {
			logJobError(`${error}`);
		}
	});

	let info = await transporter.sendMail({
		from: from,
		to: to,
		subject: subject,
		// text: text,
		html: html,
		//attachments: attachments
	})
	.catch(error => {
		logJobError(`${error}\r\n`);
		process.exit(1);
		return false;
	});
 	console.log("\nMail sent: %s", info.messageId);

}

/**
 */
function htmltpl(title, body) {
	return `<html><head><meta charset="utf-8"><title>${title}</title></head><body><div style="font-family:'Open Sans';font-weight: 400;font-size: 16px;color:#0A4370;">${body}</div></body></html>`;
}

/**
 */
// const copyGisProblemParcels = () => {
	
// 	// destination.txt will be created or overwritten by default
// 	fs.copyFileSync(_missed_gis_hdr_rpt, `${_missing_gis_rpt}`, (error) => {
// 		if (error) 
// 			logJobError(`${error}`);	
// 	});

// };

/**
 */
const logGisProblemParcels = () => {
	
	try {
		// system report. space before after 1.)
		fs.writeFileSync(_missed_gis_hdr_rpt, `\n 1.) ${_parcelsGisProblem.length} parcels have no GIS features\n`);
		// user report
		fs.writeFileSync(_missing_gis_rpt, `${_parcelsGisProblem.length} parcels have no GIS features\n\n`);
		
		let content = _parcelsGisProblem.map((i) => 
  		`${i}`
		).join('\n');

		appendFile(_missing_gis_rpt, `${content}`);

	} catch (error) {
		logJobError(`${error}`);
	}
};

/**
 */
const logJobSummary = () => {
	
	try {

		var tsEnd = new Date();
		_jobStatus.push(`Aerial Maker completed at: ${tsEnd.toLocaleTimeString('en-US')}`);

		var diff =(tsEnd.getTime() - tsStart.getTime()) / 1000;

		var days = Math.floor(diff / (3600*24));
		var hours = Math.floor(diff % (3600*24) / 3600);
		var minutes = Math.floor(diff % 3600 / 60);
		var seconds = Math.floor(diff % 60);

		var dDisplay = days > 0 ? days + (days == 1 ? " day, " : " days, ") : "";
		var hDisplay = hours > 0 ? hours + (hours == 1 ? " hour, " : " hours, ") : "";
		var mDisplay = minutes > 0 ? minutes + (minutes == 1 ? " minute, " : " minutes, ") : "";
		var sDisplay = seconds > 0 ? seconds + (seconds == 1 ? " second" : " seconds") : "";
		var duration = singleLineString`${dDisplay} ${hDisplay} ${mDisplay} ${sDisplay}`;

		if(!(duration.length>0))
			duration="less than 1 second."

			_jobStatus.push(`Aerial Maker duration: ${duration}.`);
		
		//if(_parcelsGisProblem.length>0) {
			logGisProblemParcels();
			// copyGisProblemParcels();
		//}

		let content = singleLineString`Aerial Maker results for ${_logFileDateTime}\nEnvironment: ${IMAGE_ENV}\n\r
			${found()}
			${success()}
			${missed()}
			${writeErrorSummary()}
			${jobStatusReport()}
			`;
			appendFile(_aerialmaker_export_log, `${content}\r\n`);

	} catch (error) {
		logJobError(`${error}`);
	}

};

/**
 write error to file with timestamp 
 */
const logJobError = (error) => {

	var d = new Date();
	var t = [d.getFullYear(), d.getMonth() +1, d.getDate()]; // get date
	var dts = t.map((t) => t.toString().padStart(2, '0')).join('-'); // pad leading zeros
	t = [d.getHours(), d.getMinutes(), d.getSeconds()]; // get time
	dts += '\xa0' + t.map((t) => t.toString().padStart(2, '0')).join(':');

	let content = `${dts}\xa0${IMAGE_ENV}\xa0${error}`;
	_error_summary.push(content);

	if(isDebugMode)
		console.error(content);

	appendFile(_aerialmaker_error_log, `${content}\n`);

};

/**
 */
const getParcelsToRun = () => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.extendedMetaData = true;

	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		
		var bindvars = {	
			map_year: _mapYear,
			fetchrows: _rowsToFetch,
			offset: _offset,
			cursor:  { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
		}
		
		const result = connection.execute(
		  "BEGIN UTILS.GET_PARCELS_FOR_AERIAL_EXPORT(:map_year, :fetchrows, :offset, :cursor); END;",  // The PL/SQL has an OUT bind of type SYS_REFCURSOR
			bindvars,
    )
		.then(result => {
			
			fetchRowsFromRS(connection, result.outBinds.cursor, _rowsToFetch)
			.then(result => {
				connection.close();
				console.log(`Parcels to run: ${_parcelsList.length}`);
				resolve(true);
			});

		})
		.catch( error => {			
			connection.close();
			logJobError(`getParcelsToRun(): ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		logJobError(`${error}`);
	});
});

/**
 */
const fetchRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();			
			logJobError(`fetchRowsFromRS->getRows(): ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {
				// debug
				//if(row.PARID === '1036306059')
					_parcelsList.push(row.PARID);
			});
			resultSet.close();
			resolve(rows.length);
			//fetchRowsFromRS(connection, resultSet, numRows);  // get next set of rows
		}
	});
});

/**
 */
async function insertIdocs(parid) {

	let promise = new Promise((resolve, reject) => { 

		let connection;

		oracledb.getConnection(
		{
			user : dbConfig.user,
			password : dbConfig.password,
			connectString : dbConfig.connectString
		})
		.then(c => {
			
			connection = c;
			var createdOn = new Date();

			let sql = `insert into idocs.idocx_document (id, class, version, created_by, created_on, last_updated_on, file_name, external_reference, file_size_in_bytes, content, thumbnail_content, document_type, document_system, rank, filesystem_id, process)
				values (idocs.hibernate_sequence.nextval, :class, :version, :created_by, :created_on, null, :file_name, null, :file_size_in_bytes, null, null, :document_type, :document_system, :rank, :filesystem_id, null) RETURNING id into :newid`;

			let options = {
				autoCommit: true
			};

			return connection.execute(
				sql,
				{
					class: IdocxDocument.Class,
					version: IdocxDocument.Version,
					created_by: 'AERIALMAKER',
					created_on: createdOn,
					file_name: IdocxDocument.FileName,
					file_size_in_bytes: IdocxDocument.FileSizeInBytes,
					document_type: IdocxDocumentType.Id,
					document_system: IdocxDocumentType.DocumentSystem,
					rank: IdocxDocument.Rank,
					filesystem_id: IdocxDocument.FileSystemId,
					//process: IdocxDocument.Process,				
					newid: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
				},
				options
			)
			.then(result => {
				for (let i = 0; i < result.outBinds.newid.length; i++) {
					var doc_id = result.outBinds.newid[i];
					console.log(`Added idocx_document: parid: ${parid}, Id: ${doc_id}`);
					insertIdocxField(parid, doc_id, createdOn)
					.then(result => {
						connection.close();		
						resolve(true);
					})
					.catch( error => {
						connection.close();					
						logJobError(`insertIdocxField() on parid: ${parid}: ${error}`);
						reject(error);
					});				
				}
			})
			.catch( error => {
				logJobError(`${error}`);
				reject(error);
			});
		});
	});

	let result = await promise;
	return result;

};

/**
 */
const insertIdocxField = (parid, document, createdOn) => new Promise((resolve, reject) => {

 	let connection;

	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(c => {
		
		connection = c;

		let sql = `insert into idocs.idocx_field (ID, CLASS, VERSION, CREATED_BY, CREATED_ON, DOCUMENT, NAME, PROPERTY_TYPE, LIST_PROPERTY_ITEM, STRING_VALUE, INTEGER_VALUE, DATE_VALUE)
		  					values (idocs.HIBERNATE_SEQUENCE.nextval, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)`;
	
		var createdBy = 'AERIALMAKER';
		
		let binds = [
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'Captured By', 30, null, 'sysadmin	', null, null],
				['Tyler.DocumentManager.Domain.IntegerField', 1, createdBy, createdOn, document, 'Card', 5,	null, null,	1, null],
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'Jur', 3, null, 	'51',	null, null],
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'Notes', 6, null, null, null, null],
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'Parid', 2, null, parid.toString(), null, null],
				['Tyler.DocumentManager.Domain.DateField', 		1, createdBy, createdOn, document, 'Photo Capture Date', 29, null, null, null, null],
				['Tyler.DocumentManager.Domain.ListField', 		1, createdBy, createdOn, document, 'Photo Category', 8, 18, 'Aerial',	null,	null],
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'SubjectXCoord', 72854, null, null, null, null],
				['Tyler.DocumentManager.Domain.StringField', 	1, createdBy, createdOn, document, 'SubjectYCoord', 72855, null, null, null,null],
				['Tyler.DocumentManager.Domain.IntegerField', 1, createdBy, createdOn, document, 	'Taxyr', 4,	null, null,	_mapYear, null]
			];

		let options = {
			autoCommit: true,
      bindDefs: [
        { type: oracledb.DB_TYPE_NVARCHAR, maxSize: 255 }, 	// CLASS
				{ type: oracledb.NUMBER },													// VERSION
				{ type: oracledb.DB_TYPE_NVARCHAR, maxSize: 255 }, 	// CREATED_BY
				{ type: oracledb.DB_TYPE_TIMESTAMP_LTZ},						// CREATED_ON
				{ type: oracledb.NUMBER },													// DOCUMENT
				{ type: oracledb.DB_TYPE_NVARCHAR, maxSize: 50 }, 	// NAME
				{ type: oracledb.NUMBER },													// PROPERTY_TYPE
				{ type: oracledb.NUMBER },													// LIST_PROPERTY_ITEM
				{ type: oracledb.DB_TYPE_NVARCHAR, maxSize: 2000 }, // STRING_VALUE
				{ type: oracledb.NUMBER },													// INTEGER_VALUE
				{ type: oracledb.DB_TYPE_TIMESTAMP_LTZ }						// DATE_VALUE
      ]			
		};

		connection.executeMany(sql, binds, options)
		.then(result => {			
			connection.close();
			console.log(`Added idocx_field: parid: ${parid}, document: ${document}`);
			let	ts = new Date().toLocaleTimeString('en-US');
			console.log(`Time: ${ts}`);

			resolve(result.rowsAffected);
		})
		.catch( error => {			
			logJobError(`executeMany() on parid: ${parid}: ${error}`);
			reject(error);
		});
	});
});

/**
 */
function writeErrorSummary(html_format=false) {

	if(_error_summary.length>0) {

		let content = `<b>Aerial Maker errors: ${_error_summary.length}</b>\xa0Check log.`;
		try {
			// content += _error_summary.map((v, i) => 
  		// 	`${i+1}: ${v}`
			// ).join('');
	
		} catch (error) {
			console.log(`writeErrorSummary(): ${error}`);
			content = '';
		}	

		if(html_format) {
			return `<div><br /><li>${content}</li></div>`;
		} else {
			return content;
		}

	} else {
		return '';
	}
}

/**
 */
function jobLogReportForMapping(html_format=false) {
	
 // ${".".repeat(150)}${br}

if(_parcelsGisProblem.length>0) {

		var br = '\n';
		if(html_format) {
			br = '<br />'
		}
		// let content=`<b>GIS Problem Parcels Logs</b>${br}${br}`;
		let content="";
		try {

			let _log = [];

			// log for mapping
			if (fs.existsSync(_missed_gis_hdr_rpt))
				_log.push(_missing_gis_rpt);

			content += _log.map((v) => 
				`${v}`
			).join(`${br}`);
		
		} catch (error) {
			logJobError(`jobLogReportForMapping(): ${error}`);
			content = '';
		}	
		if(html_format) {
			return `<div><br /><li>${content}</li></div>`;
		} else {
			return content;
		}
	} else {
		return '';
	}
}

/**
 */
function jobLogReport(html_format=false) {

	var br = '\r\n';
	if(html_format) {
		br = '<br />'
	}
	let content=`<b>Aerial Maker logs</b>${br}${br}`;
	try {

		let _log = [];

		// log for IT
		if (fs.existsSync(_aerialmaker_export_log))
			_log.push(_aerialmaker_export_log);

		// log for IT
		if (fs.existsSync(_aerialmaker_error_log))
			_log.push(_aerialmaker_error_log);

		// log for mapping
		if (fs.existsSync(_missed_gis_hdr_rpt)) {
			_log.push(_missed_gis_hdr_rpt);
		}

		content += _log.map((v) => 
  		`${v}`
		).join(`${br}`);
	
	} catch (error) {
		logJobError(`jobLogReport(): ${error}`);
		content = '';
	}	
	if(html_format) {
		return `<div><br /><li>${content}</li></div>`;
	} else {
		return content;
	}
}

/**
 */
function jobStatusReport(html_format=false) {

	if(_jobStatus.length>0) {

		var br = '\r\n';
		if(html_format) {
			br = '<br />'
		}
		let content=`<b>Aerial Maker runtime report</b>${br}${br}`;
		try {
			content += _jobStatus.map((v) => 
  			`${v}`
			).join(`${br}`);
	
		} catch (error) {
			logJobError(`jobStatusReport(): ${error}`);
			content = '';
		}

		if(html_format) {
			return `<div><br /><li>${content}</li></div>`;
		} else {
			return content;
		}

	} else {
		return '';
	}
}

async function run() {

	let promise = new Promise((resolve, reject) => {
				
		getMapYear()
		.then(response => {
			_mapYear = Number.parseInt(response);
			if (Number.isNaN(_mapYear)) {
				_mapYear = 0;
			}	
		})	
		.then(() => setDocumentType())
		.then(response => {
			return doWorkflow();
		})
		.then(logJobSummary)
		.then(mailLogs)
		.then(response => {
			resolve(response);
		})
		.catch( error => {			
			logJobError(`Run(): ${error}`);
			mailLogs();
			reject(false);
		});
	});

	let result = await promise;
	return result;
	
}

/* ***********************************************************
* Start here
* Add globals here
* 
* Semantics:
* missed -> gis_problem -> refers to GIS Parcel Problems
* 
* 
************************************************************ */

// gis endpoints
let _mapLayerAndLabelsUrl 		= "https://gis.xxx.com/arcgis/rest/services/Website/WebLayers/MapServer";
let _latestYearAerialUrl 			= "https://www.xxx.org/gisimg/rest/services/current/aerials/ImageServer";
let _printServiceUrl 					= "https://gis.xxx.com/arcgis/rest/services/Website/AerialMaker/GPServer/Export%20Web%20Map";
let _map_meta_url 						= 'https://www.xxx.org/gisimg/rest/services/current/aerials/ImageServer?f=json';

// current runtime environment
const IMAGE_ENV 							= "";

// current runtime aerial images saveto folder
var _exportfolder 						= `\\\\${IMAGE_ENV}\\Photos\\`;
let logs_parent_folder				= '\\\\Aerialmaker\\';

// GIS parcel problems log will go here
let _mapping_folder						= '\\\\Mapping\\Reports';
let _missing_folder						= '\\\\gis_reports';

/* ******************************************
log name 	= _missing_gis_rpt
file name = 1_missing_gis_feature.txt
location	= put in _mapping_folder
format 		= 1 blank line at top
****************************************** */
let _missing_gis_rpt_fn				= '1_missing_gis_features.txt';
let _missing_gis_rpt 					= `${_mapping_folder}\\${_missing_gis_rpt_fn}`;

/* ******************************************
log name	= _missed_gis_hdr_rpt
file name = missing_gis_features_header.txt
location	= put in _missing_folder
format 		= 1 blank line before and after
						header message
****************************************** */
let _missed_gis_hdr_fn				= `missing_gis_features_header.txt`;
let _missed_gis_hdr_rpt				= `${_missing_folder}\\${_missed_gis_hdr_fn}`;

// log names
let _export_logs 							= `aerialmaker-export-${_logFileDate}.log`;
let _error_logs 							= `aerialmaker-error-${_logFileDate}.log`;

// log sub folders
let _aerialmaker_export_log 	= `${logs_parent_folder}export_logs\\${_export_logs}`;
let _aerialmaker_error_log		= `${logs_parent_folder}error_logs\\${_error_logs}`;


// collection of errors for logs
var _error_summary = [];
var _parcelsList = [];
var _parcelsGisProblem = [];
var _jobStatus = [];
var _mapYear = '';
let _rowsToFetch = 200000;
let _offset = 0;

var IdocxDocumentType = {
	Id: '',
	Version: '',
	DocumentSystem: '',
	FieldType: '',
}

var IdocxDocument = { 
	Class: 'Tyler.DocumentManager.Domain.Document',
	Version: 1,
	CreatedBy: 'sysadmin',
	CreatedOn: '',
	LastUpdatedOn: '',
	FileName: '',
	ExternalReference: '',
	FileSizeInBytes: '',
	DocumentType: '',
	DocumentSystem: '',
	Rank: 9999,
	FileSystemId: '',
	Process: ''
}

_jobStatus.push(`Aerial Maker is using oracledb.version ${oracledb.versionString}. ${(parseFloat(oracledb.versionString) >= 4.2 ? 'OK' : 'Should be 4.2 or greater.')}`); // use version 4.2 or greater
_jobStatus.push(`Aerial Maker started at: ${tsStart.toLocaleTimeString('en-US')}`);

if(1===1) {

	run()
	.then((result) => {
		console.log(`run() -> result: ${result}`);
		return result;
	})
	.catch(error => {
			logJobError(`start(): ${error}`);
	});	

}

// for debugging //throw new Error('something bad happened');