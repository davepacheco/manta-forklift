#!/usr/bin/env node
/* vim: set ts=8 sts=8 sw=8 noet: */

var mod_crypto = require('crypto');
var mod_path = require('path');
var mod_fs = require('fs');
var mod_events = require('events');
var mod_util = require('util');

var mod_assert = require('assert-plus');
var mod_extsprintf = require('extsprintf');
var mod_once = require('once');
var mod_manta = require('manta');
var mod_portmutex = require('portmutex');
var mod_vasync = require('vasync');

var sprintf = mod_extsprintf.sprintf;


var CONFIG;
var CLIENT;
var MUTEX;
var UPLOADING = null;

/*
 * We track files that we know about in a round-robin loop.  Each one
 * is uploaded in turn; if we fail for any reason we put it at the end
 * of the list and try the next one.  This way, files with transient
 * failures will eventually be uploaded and files with potentially
 * permanent failures will not block the queue.
 */
var FILEQ = [];

function
log()
{
	var now = new Date();
	var datestr = sprintf('[%04d-%02d-%02d %02d:%02d:%02d]',
	    now.getFullYear(), now.getMonth() + 1, now.getDate(),
	    now.getHours(), now.getMinutes(), now.getSeconds());
	var msgstr = sprintf.apply(null, arguments);
	process.stderr.write(datestr + ' ' + msgstr + '\n');
}

function
b64_to_md5(b64)
{
	return (new Buffer(b64, 'base64').toString('hex'));
}

function
load_config()
{
	try {
		var path = mod_path.join(__dirname, 'config.json');
		var str = mod_fs.readFileSync(path, 'utf8');
		var json = JSON.parse(str);

		mod_assert.string(json.manta_dir, 'config needs "manta_dir"');
		mod_assert.string(json.local_dir, 'config needs "local_dir"');
		mod_assert.arrayOfString(json.match_filenames, 'config needs ' +
		    '"match_filenames"');
		mod_assert.object(json.manta, 'config needs "manta"');
		mod_assert.string(json.manta.user, 'config needs ' +
		    '"manta.user"');
		mod_assert.string(json.manta.key_id, 'config needs ' +
		    '"manta.key_id"');
		mod_assert.string(json.manta.key_file, 'config needs ' +
		    '"manta.key_file"');
		mod_assert.number(json.lockport, 'config needs "lockport"');

		json.local_dir = json.local_dir.replace(/^~\//,
		    process.env.HOME + '/');
		json.manta.key_file = json.manta.key_file.replace(/^~\//,
		    process.env.HOME + '/');

		json.match_filenames = json.match_filenames.map(function (x) {
			return (new RegExp(x));
		});

		if (json.manta.key_file[0] && json.manta.key_file[0] !== '/') {
			json.manta.key_file = mod_path.join(__dirname,
			    json.manta.key_file);
		}

		json.manta.key = mod_fs.readFileSync(json.manta.key_file, 'utf8');

		return (json);
	} catch (ex) {
		console.error('ERROR: loading config file');
		console.error(ex.stack);
		process.exit(1);
	}
}

function
fs_dir_list(callback)
{
	callback = mod_once(callback);

	var file_list = [];

	mod_fs.readdir(CONFIG.local_dir, function (err, files) {
		if (err) {
			callback(err);
			return;
		}

		files = files.filter(filename_filter);
		if (files.length < 1) {
			handle_files();
			return;
		}

		var b = mod_vasync.barrier();
		b.on('drain', handle_files);

		files.forEach(function (file) {
			var path = mod_path.join(CONFIG.local_dir, file);
			b.start('lstat ' + path);
			mod_fs.lstat(path, function (_err, _stat) {
				if (_err) {
					callback(_err);
					return;
				}

				if (_stat.isFile())
					file_list.push(path);

				b.done('lstat ' + path);
			});
		});
	});

	function filename_filter(fn) {
		for (q = 0; q < CONFIG.match_filenames.length; q++)
			if (CONFIG.match_filenames[q].test(fn))
				return (true);
		return (false);
	}

	function handle_files() {
		/*
		 * Prune any entries in the FILEQ that no longer appear
		 * in the directory:
		 */
		for (var i = 0; i < FILEQ.length; i++) {
			if (file_list.indexOf(FILEQ[i]) === -1) {
				log('file went away: %s', FILEQ[i]);
				FILEQ.splice(i, 1);
			}
		}
		/*
		 * Add all new entries to the end of the list:
		 */
		for (var i = 0; i < file_list.length; i++) {
			if (UPLOADING !== file_list[i] &&
			    FILEQ.indexOf(file_list[i]) === -1) {
				log('saw new file: %s', file_list[i]);
				FILEQ.push(file_list[i]);
			}
		}

		callback(null);
	}
}

function
upload_end(remove_local_file)
{
	if (remove_local_file) {
		log('delete: %s', UPLOADING);
		try {
			mod_fs.unlinkSync(UPLOADING);
		} catch (ex) {
			log('delete error: %r', ex);
		}
	}
	UPLOADING = null;
	setTimeout(upload_one, 1000);
}

function
upload_one()
{
	if (UPLOADING !== null || FILEQ.length < 1)
		return;

	UPLOADING = FILEQ.shift();
	log('uploading: %s', UPLOADING);

	var manta_path = mod_path.join(CONFIG.manta_dir,
	    mod_path.basename(UPLOADING));

	var options = {
		copies: 2,
		headers: {
			/*
			 * Only upload if object does not exist:
			 */
			'if-match': '""'
		}
	};

	var fstream = mod_fs.createReadStream(UPLOADING);
	var ended = false;
	fstream.on('error', function (err) {
		log('file read error (%s): %r', UPLOADING, err);
		if (!ended) {
			ended = true;
			upload_end(false);
		}
	});
	fstream.on('open', function () {
		CLIENT.put(manta_path, fstream, options, function (err, res) {
			var wasended = ended;
			ended = true;
			if (err && err.name === 'PreconditionFailedError') {
				log('file exists: %s', manta_path);
				if (!wasended)
					handle_conflict(manta_path);
			} else if (err) {
				log('upload error: %r', err);
				if (!wasended)
					upload_end(false);
			} else {
				if (!wasended)
					upload_end(true);
			}
		});
	});
}

function
md5sum(fullpath, callback)
{
	callback = mod_once(callback);
	var summer = mod_crypto.createHash('md5');
	var fstream = mod_fs.createReadStream(fullpath);

	fstream.on('data', function (ch) {
		summer.update(ch);
	});
	fstream.on('error', function (err) {
		callback(err);
	});
	fstream.on('end', function () {
		callback(null, summer.digest('hex'));
	});
}

function
handle_conflict(manta_path)
{
	/*
	 * Get the MD5 sum of the remote file:
	 */
	CLIENT.info(manta_path, function(err, info) {
		if (err) {
			log('info error: %r', err);
			upload_end(false);
			return;
		}

		var remote_md5 = b64_to_md5(info.md5);

		md5sum(UPLOADING, function (err, md5) {
			if (err) {
				log('md5 error: %r', err);
				upload_end(false);
				return;
			}

			if (md5 !== remote_md5) {
				log('mismatched local and remote: %s',
				    UPLOADING);
				upload_end(false);
				return;
			}

			log('checksum match: %s', UPLOADING);
			upload_end(true);
		});
	});
}

function
dispatcher()
{
	fs_dir_list(function (err) {
		if (err) {
			log('error listing local files: %r' + err);
		}
		setTimeout(dispatcher, 10000);
		upload_one();
	});
}

function
create_manta_client(callback)
{
	var client = mod_manta.createClient({
		sign: mod_manta.privateKeySigner({
			key: CONFIG.manta.key,
			keyId: CONFIG.manta.key_id,
			user: CONFIG.manta.user
		}),
		user: CONFIG.manta.user,
		url: CONFIG.manta.url,
		connectTimeout: 2000
	});
	client.on('error', function (err) {
		log('MANTA CLIENT ERROR: %r', err);
		process.exit(1);
	});
	mod_assert.ok(client, 'manta client');
	callback(client);
}

/*
 * Main code:
 */

CONFIG = load_config();

MUTEX = new mod_portmutex.PortMutex(CONFIG.lockport, 1000);
MUTEX.on('retrying', function () {
	log('could not get port lock (port %d); aborting!', CONFIG.lockport);
	process.exit(1);
});
MUTEX.on('gotlock', function () {
	log('ok, starting up');
	create_manta_client(function (client) {
		CLIENT = client;

		dispatcher();
	});
});
