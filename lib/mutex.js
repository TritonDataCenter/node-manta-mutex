/* vim: set ts=8 sts=8 sw=8 noet: */

var mod_fs = require('fs');
var mod_util = require('util');
var mod_events = require('events');

var mod_assert = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_readtoend = require('readtoend');

var VError = mod_verror.VError;

var MAGIC_LENGTH_RANDOM = 8 * 8;
var MAGIC_START = 'MAGIC:';
var MAGIC_END = ':CIGAM';
var MAGIC_LENGTH = MAGIC_START.length + MAGIC_LENGTH_RANDOM + MAGIC_END.length;

function
random_hex()
{
	var s = ((Math.random() * 0xffffffff) >>> 0).toString(16);
	while (s.length < 8)
		s = '0' + s;
	return (s);
}

function
make_magic()
{
	mod_assert.strictEqual(MAGIC_LENGTH_RANDOM % 8, 0);

	var m = '';
	while (m.length !== MAGIC_LENGTH_RANDOM) {
		m += random_hex();
	}

	var mm = MAGIC_START + m + MAGIC_END;
	mod_assert.strictEqual(mm.length, MAGIC_LENGTH);

	return (mm);
}

function
open_magic_file(self, next)
{
	if (self.mm_magic !== null) {
		next();
		return;
	}

	mod_fs.readFile(self.mm_local_path, {
		encoding: 'utf8'
	}, function (err, data) {
		if (err) {
			if (err.code === 'ENOENT') {
				next();
			} else {
				next(new VError(err, 'could not open lock ' +
				    'file "%s"', self.mm_local_path));
			}
			return;
		}

		mod_assert.string(data);
		data = data.trim();
		if (data.length === MAGIC_LENGTH &&
		    mod_jsprim.startsWith(data, MAGIC_START) &&
		    mod_jsprim.endsWith(data, MAGIC_END)) {
			/*
			 * It's our magic, so we should use it.
			 */
			self.mm_magic = data;
			next();
			return;
		}

		/*
		 * It is not our magic, so we should error out.
		 */
		next(new VError(err, 'did not recognise magic in ' +
		    'lock file "%s", is it stale or not ours?',
		    self.mm_local_path));
	});
}

function
create_magic_file(self, next)
{
	if (self.mm_magic !== null) {
		next();
		return;
	}

	/*
	 * We did not read magic from a file, so we must open a new magic file.
	 */
	mod_assert.strictEqual(self.mm_wstr, null);
	self.mm_wstr = mod_fs.createWriteStream(self.mm_local_path, {
		flags: 'wx',
		encoding: 'utf8'
	});

	self.mm_wstr.on('open', function (fd) {
		/*
		 * Save the file descriptor so that we may fsync(2) after the
		 * write completes:
		 */
		mod_assert.number(fd);
		mod_assert.strictEqual(self.mm_fd, -1);
		self.mm_fd = fd;
	});

	/*
	 * Generate magic to write:
	 */
	var magic = make_magic();
	self.mm_wstr.write(magic, function () {
		mod_assert.number(self.mm_fd);
		mod_assert.notStrictEqual(self.mm_fd, -1);

		mod_fs.fsync(self.mm_fd, function (err) {
			/*
			 * Whether this was a success or not, we must close
			 * the stream now:
			 */
			self.mm_wstr.end();
			self.mm_wstr = null;
			self.mm_fd = -1;

			if (err) {
				next(new VError(err, 'could not fsync "%s"',
				    self.mm_local_path));
				return;
			}

			mod_assert.strictEqual(self.mm_magic, null);
			self.mm_magic = magic;

			next();
		});
	});
}

function
read_lock_object(self, next)
{
	var data;
	var etag;

	var done = false;
	var endfn = function (err) {
		if (done)
			return;
		done = true;
		next(err);
	};

	var b = mod_vasync.barrier();
	b.start('etag');
	b.start('data');

	var rs = self.mm_manta.createReadStream(self.mm_remote_path);
	rs.on('error', function (err) {
		if (err.name === 'ResourceNotFoundError') {
			/*
			 * Skip out so that we can try and _write_ the file.
			 */
			endfn();
		} else {
			endfn(new VError(err, 'could not read lock object'));
		}
	});
	rs.on('close', function (res) {
		if (res && res.headers && res.headers.etag) {
			etag = res.headers.etag.trim();
			b.done('etag');
		} else {
			endfn(new VError('no etag on request'));
		}
	});

	mod_readtoend.readToEnd(rs, function (err, body) {
		if (err) {
			endfn(new VError(err, 'could not read ' +
			    'object "%s"', self.mm_remote_path));
			return;
		}
		data = body.trim();
		b.done('data');
	});

	b.on('drain', function () {
		if (done)
			return;

		mod_assert.string(data);
		mod_assert.string(etag);
		mod_assert.ok(data && etag);

		mod_assert.strictEqual(self.mm_remote_magic, null);
		self.mm_remote_magic = data.trim();

		mod_assert.strictEqual(self.mm_remote_etag, null);
		self.mm_remote_etag = etag;

		endfn();
	});
}

function
put_lock_object(self, next)
{
	if (self.mm_remote_magic) {
		/*
		 * We were able to read magic from Manta, so we needn't
		 * try and write the file.
		 */
		next();
		return;
	}

	mod_assert.strictEqual(self.mm_wstr, null);
	self.mm_wstr = self.mm_manta.createWriteStream(self.mm_remote_path, {
		headers: {
			'if-match': '""'
		}
	});
	self.mm_wstr.once('error', function (err) {
		self.mm_wstr.removeAllListeners();
		next(new VError(err, 'could not write object "%s"',
		    self.mm_remote_path));
	});
	self.mm_wstr.once('close', function (res) {
		mod_assert.string(self.mm_magic);
		mod_assert.ok(!self.mm_remote_magic);

		mod_assert.object(res);
		mod_assert.object(res.headers);
		mod_assert.string(res.headers.etag);

		self.mm_remote_etag = res.headers.etag;
		self.mm_remote_magic = self.mm_magic;

		self.mm_wstr.removeAllListeners();
		next();
	});

	self.mm_wstr.write(self.mm_magic);
	self.mm_wstr.end();
}

function
remove_lock_object(self, next)
{
	mod_assert.string(self.mm_remote_etag);

	self.mm_manta.unlink(self.mm_remote_path, {
		headers: {
			'if-match': self.mm_remote_etag
		}
	}, function (err) {
		if (err) {
			next(new VError(err, 'could not unlink "%s"',
			    self.mm_remote_path));
			return;
		}
		next();
	});
}

function
MantaMutex(options)
{
	mod_assert.object(options, 'options');
	mod_assert.object(options.manta, 'options.manta');
	mod_assert.string(options.remote_path, 'options.remote_path');
	mod_assert.string(options.local_path, 'options.local_path');
	mod_assert.optionalNumber(options.timeout, 'options.timeout');
	mod_assert.optionalNumber(options.retry_delay, 'options.retry_delay');

	var self = this;
	mod_events.EventEmitter.call(self);

	self.mm_manta = options.manta;

	self.mm_remote_path = options.remote_path;
	self.mm_local_path = options.local_path;

	self.mm_timeout = options.timeout || Infinity;
	self.mm_retry_delay = options.retry_delay || 5000;

	self.mm_lock_called = false;
	self.mm_locked = false;
	self.mm_release_called = false;
	self.mm_released = false;

	self.mm_pipeline = null;
	self.mm_wstr = null;
	self.mm_magic = null;
	self.mm_fd = -1;

	self.mm_remote_magic = null;
	self.mm_remote_etag = null;
}
mod_util.inherits(MantaMutex, mod_events.EventEmitter);

MantaMutex.prototype.release = function
release(callback)
{
	var self = this;

	mod_assert.ok(self.mm_locked, 'cannot release before lock!');
	mod_assert.ok(!self.mm_release_called, 'release cannot be ' +
	    'called again');

	self.mm_release_called = true;

	mod_assert.optionalFunc(callback, 'callback');
	if (callback)
		self.once('released', callback);

	remove_lock_object(self, function (err) {
		if (err) {
			self.emit('error', new VError(err, 'release failure'));
			return;
		}

		mod_assert.ok(!self.mm_released);
		self.mm_released = true;
		self.emit('released');
	});
};

MantaMutex.prototype.lock = function
lock(callback)
{
	var self = this;

	mod_assert.ok(!self.mm_lock_called, 'lock cannot be called again');
	self.mm_lock_called = true;

	mod_assert.optionalFunc(callback, 'callback');
	if (callback)
		self.once('released', callback);

	self.mm_pipeline = mod_vasync.pipeline({
		funcs: [
			/*
			 * Open (or create) the magic file:
			 */
			open_magic_file,
			create_magic_file,

			/*
			 * Write the lock object to Manta:
			 */
			read_lock_object,
			put_lock_object
		],
		arg: self
	}, function (err) {
		if (err) {
			self.emit('error', new VError(err, 'locking failure'));
			return;
		}

		mod_assert.string(self.mm_magic);
		mod_assert.string(self.mm_remote_magic);
		mod_assert.string(self.mm_remote_etag);

		/*
		 * If we fetched (or wrote) the correct magic, then inform
		 * the consumer that they have the lock.
		 */
		if (self.mm_remote_magic === self.mm_magic) {
			self.mm_locked = true;
			self.emit('locked');
			return;
		}

		/*
		 * Otherwise, there was a locking conflict.
		 */
		if (self.listeners('conflict').length !== 0) {
			self.emit('conflict', {
				remote: {
					magic: self.mm_remote_magic,
					path: self.mm_remote_path
				},
				local: {
					magic: self.mm_magic,
					path: self.mm_local_path
				}
			});
		} else {
			self.emit('error', new VError(
			    'lock conflict: local file "%s", object "%s"',
			    self.mm_local_path, self.mm_remote_path));
		}
	});
};

module.exports = {
	MantaMutex: MantaMutex
};
