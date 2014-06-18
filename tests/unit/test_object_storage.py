#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mimetypes
import random
import time
import unittest

from six import StringIO

from mock import patch
from mock import MagicMock as Mock

import pyrax
import pyrax.object_storage
from pyrax.object_storage import ACCOUNT_META_PREFIX
from pyrax.object_storage import assure_container
from pyrax.object_storage import Container
from pyrax.object_storage import CONTAINER_META_PREFIX
from pyrax.object_storage import Fault_cls
from pyrax.object_storage import FAULT
from pyrax.object_storage import get_file_size
from pyrax.object_storage import OBJECT_META_PREFIX
from pyrax.object_storage import _massage_metakeys
from pyrax.object_storage import StorageClient
from pyrax.object_storage import StorageObject
from pyrax.object_storage import StorageObjectIterator
from pyrax.object_storage import _validate_file_or_path
import pyrax.exceptions as exc
import pyrax.utils as utils

import pyrax.fakes as fakes



class ObjectStorageTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ObjectStorageTest, self).__init__(*args, **kwargs)
        self.identity = fakes.FakeIdentity()
        self.maxDiff = 1000

    def setUp(self):
        self.client = fakes.FakeStorageClient(self.identity)
        self.container = self.client.create("fake")
        nm = "fake_object"
        ctype = "text/fake"
        self.obj = StorageObject(self.container.object_manager,
                {"name": nm, "content_type": ctype})

    def tearDown(self):
        pass

    def test_fault(self):
        f = Fault_cls()
        self.assertFalse(f)

    def test_assure_container(self):
        class TestClient(object):
            _manager = fakes.FakeManager()

            @assure_container
            def test_method(self, container):
                return container

        client = TestClient()
        client.get = Mock(return_value=self.container)
        # Pass the container
        ret = client.test_method(self.container)
        self.assertTrue(ret is self.container)
        # Pass the name
        ret = client.test_method(self.container.name)
        self.assertTrue(ret is self.container)

    def test_massage_metakeys(self):
        prefix = "ABC-"
        orig = {"ABC-yyy": "ok", "zzz": "change"}
        expected = {"ABC-yyy": "ok", "ABC-zzz": "change"}
        fixed = _massage_metakeys(orig, prefix)
        self.assertEqual(fixed, expected)

    def test_validate_file_or_path(self):
        obj_name = utils.random_unicode()
        with utils.SelfDeletingTempfile() as tmp:
            ret = _validate_file_or_path(tmp, obj_name)
        self.assertEqual(ret, obj_name)

    def test_validate_file_or_path_not_found(self):
        pth = utils.random_unicode()
        obj_name = utils.random_unicode()
        self.assertRaises(exc.FileNotFound, _validate_file_or_path, pth,
                obj_name)

    def test_validate_file_or_path_object(self):
        pth = object()
        obj_name = utils.random_unicode()
        ret = _validate_file_or_path(pth, obj_name)
        self.assertEqual(ret, obj_name)

    def test_get_file_size(self):
        sz = random.randint(42, 420)
        fobj = StringIO("x" * sz)
        ret = get_file_size(fobj)
        self.assertEqual(sz, ret)

    @patch('pyrax.object_storage.StorageObjectManager',
            new=fakes.FakeStorageObjectManager)
    def test_container_create(self):
        api = utils.random_unicode()
        mgr = fakes.FakeManager()
        mgr.api = api
        nm = utils.random_unicode()
        info = {"name": nm}
        cont = Container(mgr, info)
        self.assertEqual(cont.manager, mgr)
        self.assertEqual(cont._info, info)
        self.assertEqual(cont.name, nm)

    def test_backwards_aliases(self):
        cont = self.container
        get_func = cont.get_objects.im_func
        list_func = cont.list.im_func
        self.assertTrue(get_func is list_func)

    def test_repr(self):
        cont = self.container
        rpr = cont.__repr__()
        self.assertTrue("Container" in rpr)
        self.assertTrue(cont.name in rpr)

    def test_id(self):
        cont = self.container
        self.assertEqual(cont.id, cont.name)
        cont.name = utils.random_unicode()
        self.assertEqual(cont.id, cont.name)

    def test_set_cdn_defaults(self):
        cont = self.container
        self.assertTrue(isinstance(cont._cdn_uri, Fault_cls))
        cont._set_cdn_defaults()
        self.assertIsNone(cont._cdn_uri)

    def test_fetch_cdn_data(self):
        cont = self.container
        self.assertTrue(isinstance(cont._cdn_uri, Fault_cls))
        cdn_uri = utils.random_unicode()
        cdn_ssl_uri = utils.random_unicode()
        cdn_streaming_uri = utils.random_unicode()
        cdn_ios_uri = utils.random_unicode()
        cdn_log_retention = random.choice(("True", "False"))
        bool_retention = (cdn_log_retention == "True")
        cdn_ttl = str(random.randint(1, 1000))
        hdrs = {"X-Cdn-Uri": cdn_uri,
                "X-Ttl": cdn_ttl,
                "X-Cdn-Ssl-Uri": cdn_ssl_uri,
                "X-Cdn-Streaming-Uri": cdn_streaming_uri,
                "X-Cdn-Ios-Uri": cdn_ios_uri,
                "X-Log-Retention": cdn_log_retention,
                }
        cont.manager.fetch_cdn_data = Mock(return_value=hdrs)
        self.assertEqual(cont.cdn_uri, cdn_uri)
        self.assertEqual(cont.cdn_uri, cdn_uri)
        self.assertEqual(cont.cdn_ttl, int(cdn_ttl))
        self.assertEqual(cont.cdn_ssl_uri, cdn_ssl_uri)
        self.assertEqual(cont.cdn_streaming_uri, cdn_streaming_uri)
        self.assertEqual(cont.cdn_ios_uri, cdn_ios_uri)
        self.assertEqual(cont.cdn_log_retention, bool_retention)

    def test_fetch_cdn_data_not_enabled(self):
        cont = self.container
        cont.manager.fetch_cdn_data = Mock(return_value={})
        ret = cont._fetch_cdn_data()
        self.assertIsNone(ret)
        self.assertIsNone(cont.cdn_uri)

    def test_cont_get_metadata(self):
        cont = self.container
        prefix = utils.random_ascii()
        cont.manager.get_metadata = Mock()
        cont.get_metadata(prefix=prefix)
        cont.manager.get_metadata.assert_called_once_with(cont, prefix=prefix)

    def test_cont_set_metadata(self):
        cont = self.container
        prefix = utils.random_ascii()
        key = utils.random_ascii()
        val = utils.random_ascii()
        metadata = {key: val}
        cont.manager.set_metadata = Mock()
        cont.set_metadata(metadata, prefix=prefix)
        cont.manager.set_metadata.assert_called_once_with(cont, metadata,
                prefix=prefix, clear=False)

    def test_cont_remove_metadata_key(self):
        cont = self.container
        prefix = utils.random_ascii()
        key = utils.random_ascii()
        cont.manager.remove_metadata_key = Mock()
        cont.remove_metadata_key(key, prefix=prefix)
        cont.manager.remove_metadata_key.assert_called_once_with(cont, key,
                prefix=prefix)

    def test_cont_set_web_index_page(self):
        cont = self.container
        page = utils.random_unicode()
        cont.manager.set_web_index_page = Mock()
        cont.set_web_index_page(page)
        cont.manager.set_web_index_page.assert_called_once_with(cont, page)

    def test_cont_set_web_error_page(self):
        cont = self.container
        page = utils.random_unicode()
        cont.manager.set_web_error_page = Mock()
        cont.set_web_error_page(page)
        cont.manager.set_web_error_page.assert_called_once_with(cont, page)

    def test_cont_make_public(self):
        cont = self.container
        ttl = utils.random_unicode()
        cont.manager.make_public = Mock()
        cont.make_public(ttl=ttl)
        cont.manager.make_public.assert_called_once_with(cont, ttl=ttl)

    def test_cont_make_private(self):
        cont = self.container
        cont.manager.make_private = Mock()
        cont.make_private()
        cont.manager.make_private.assert_called_once_with(cont)

    def test_cont_purge_cdn_object(self):
        cont = self.container
        obj = utils.random_unicode()
        email_addresses = utils.random_unicode()
        cont.object_manager.purge = Mock()
        cont.purge_cdn_object(obj, email_addresses=email_addresses)
        cont.object_manager.purge.assert_called_once_with(obj,
                email_addresses=email_addresses)

    def test_cont_get(self):
        cont = self.container
        item = utils.random_unicode()
        item_obj = utils.random_unicode()
        cont.object_manager.get = Mock(return_value=item_obj)
        ret = cont.get(item)
        self.assertEqual(ret, item_obj)

    def test_cont_list(self):
        cont = self.container
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = utils.random_unicode()
        cont.object_manager.list = Mock()
        cont.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        cont.object_manager.list.assert_called_once_with(marker=marker,
                limit=limit, prefix=prefix, delimiter=delimiter,
                end_marker=end_marker, return_raw=return_raw)

    def test_cont_list_all(self):
        cont = self.container
        prefix = utils.random_unicode()
        cont.manager.object_listing_iterator = Mock()
        cont.list_all(prefix=prefix)
        cont.manager.object_listing_iterator.assert_called_once_with(cont,
                prefix=prefix)

    def test_cont_list_object_names_full(self):
        cont = self.container
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = True
        name1 = utils.random_unicode()
        name2 = utils.random_unicode()
        obj1 = fakes.FakeStorageObject(cont.object_manager, name=name1)
        obj2 = fakes.FakeStorageObject(cont.object_manager, name=name2)
        cont.list_all = Mock(return_value=[obj1, obj2])
        nms = cont.list_object_names(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                full_listing=full_listing)
        cont.list_all.assert_called_once_with(prefix=prefix)
        self.assertEqual(nms, [name1, name2])

    def test_cont_list_object_names(self):
        cont = self.container
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = False
        name1 = utils.random_unicode()
        name2 = utils.random_unicode()
        obj1 = fakes.FakeStorageObject(cont.object_manager, name=name1)
        obj2 = fakes.FakeStorageObject(cont.object_manager, name=name2)
        cont.list = Mock(return_value=[obj1, obj2])
        nms = cont.list_object_names(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                full_listing=full_listing)
        cont.list.assert_called_once_with(marker=marker, limit=limit,
                prefix=prefix, delimiter=delimiter, end_marker=end_marker)
        self.assertEqual(nms, [name1, name2])

    def test_cont_find(self):
        cont = self.container
        cont.object_manager.find = Mock()
        key1 = utils.random_unicode()
        val1 = utils.random_unicode()
        key2 = utils.random_unicode()
        val2 = utils.random_unicode()
        cont.find(key1=val1, key2=val2)
        cont.object_manager.find.assert_called_once_with(key1=val1, key2=val2)

    def test_cont_findall(self):
        cont = self.container
        cont.object_manager.findall = Mock()
        key1 = utils.random_unicode()
        val1 = utils.random_unicode()
        key2 = utils.random_unicode()
        val2 = utils.random_unicode()
        cont.findall(key1=val1, key2=val2)
        cont.object_manager.findall.assert_called_once_with(key1=val1,
                key2=val2)

    def test_cont_create(self):
        cont = self.container
        cont.object_manager.create = Mock()
        file_or_path = utils.random_unicode()
        data = utils.random_unicode()
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        metadata = utils.random_unicode()
        chunk_size = utils.random_unicode()
        headers = utils.random_unicode()
        return_none = utils.random_unicode()
        cont.create(file_or_path=file_or_path, data=data, obj_name=obj_name,
                content_type=content_type, etag=etag,
                content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata, chunk_size=chunk_size, headers=headers,
                return_none=return_none)
        cont.object_manager.create.assert_called_once_with(
                file_or_path=file_or_path, data=data, obj_name=obj_name,
                content_type=content_type, etag=etag,
                content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata, chunk_size=chunk_size, headers=headers,
                return_none=return_none)

    def test_cont_store_object(self):
        cont = self.container
        cont.create = Mock()
        obj_name = utils.random_unicode()
        data = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        ttl = utils.random_unicode()
        return_none = utils.random_unicode()
        extra_info = utils.random_unicode()
        cont.store_object(obj_name, data, content_type=content_type, etag=etag,
                content_encoding=content_encoding, ttl=ttl,
                return_none=return_none, extra_info=extra_info)
        cont.create.assert_called_once_with(obj_name=obj_name, data=data,
                content_type=content_type, etag=etag,
                content_encoding=content_encoding, ttl=ttl,
                return_none=return_none)

    def test_cont_upload_file(self):
        cont = self.container
        cont.create = Mock()
        file_or_path = utils.random_unicode()
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        ttl = utils.random_unicode()
        return_none = utils.random_unicode()
        content_length = utils.random_unicode()
        cont.upload_file(file_or_path, obj_name=obj_name,
                content_type=content_type, etag=etag,
                content_encoding=content_encoding, ttl=ttl,
                return_none=return_none, content_length=content_length)
        cont.create.assert_called_once_with(file_or_path=file_or_path,
                obj_name=obj_name, content_type=content_type, etag=etag,
                content_encoding=content_encoding,
                content_length=content_length, ttl=ttl,
                return_none=return_none)

    def test_cont_fetch(self):
        cont = self.container
        cont.object_manager.fetch = Mock()
        obj = utils.random_unicode()
        include_meta = utils.random_unicode()
        chunk_size = utils.random_unicode()
        size = utils.random_unicode()
        extra_info = utils.random_unicode()
        cont.fetch(obj, include_meta=include_meta, chunk_size=chunk_size,
                size=size, extra_info=extra_info)
        cont.object_manager.fetch.assert_called_once_with(obj,
                include_meta=include_meta, chunk_size=chunk_size, size=size)

    def test_cont_fetch_object(self):
        cont = self.container
        cont.fetch = Mock()
        obj_name = utils.random_unicode()
        include_meta = utils.random_unicode()
        chunk_size = utils.random_unicode()
        cont.fetch_object(obj_name, include_meta=include_meta,
                chunk_size=chunk_size)
        cont.fetch.assert_called_once_with(obj=obj_name,
                include_meta=include_meta, chunk_size=chunk_size)

    def test_cont_fetch_partial(self):
        cont = self.container
        cont.object_manager.fetch_partial = Mock()
        obj = utils.random_unicode()
        size = utils.random_unicode()
        cont.fetch_partial(obj, size)
        cont.object_manager.fetch_partial.assert_called_once_with(obj, size)

    def test_cont_download(self):
        cont = self.container
        cont.object_manager.download = Mock()
        obj = utils.random_unicode()
        directory = utils.random_unicode()
        structure = utils.random_unicode()
        cont.download(obj, directory, structure=structure)
        cont.object_manager.download.assert_called_once_with(obj, directory,
                structure=structure)

    def test_cont_download_object(self):
        cont = self.container
        cont.download = Mock()
        obj_name = utils.random_unicode()
        directory = utils.random_unicode()
        structure = utils.random_unicode()
        cont.download_object(obj_name, directory, structure=structure)
        cont.download.assert_called_once_with(obj=obj_name,
                directory=directory, structure=structure)

    def test_cont_delete(self):
        cont = self.container
        cont.manager.delete = Mock()
        del_objects = utils.random_unicode()
        cont.delete(del_objects=del_objects)
        cont.manager.delete.assert_called_once_with(cont,
                del_objects=del_objects)

    def test_cont_delete_object(self):
        cont = self.container
        cont.object_manager.delete = Mock()
        obj = utils.random_unicode()
        cont.delete_object(obj)
        cont.object_manager.delete.assert_called_once_with(obj)

    def test_cont_delete_object_in_seconds(self):
        cont = self.container
        cont.manager.delete_object_in_seconds = Mock()
        obj = utils.random_unicode()
        seconds = utils.random_unicode()
        extra_info = utils.random_unicode()
        cont.delete_object_in_seconds(obj, seconds, extra_info=extra_info)
        cont.manager.delete_object_in_seconds.assert_called_once_with(cont,
                obj, seconds)

    def test_cont_delete_all_objects(self):
        cont = self.container
        cont.object_manager.delete_all_objects = Mock()
        name1 = utils.random_unicode()
        name2 = utils.random_unicode()
        async = utils.random_unicode()
        cont.list_object_names = Mock(return_value=[name1, name2])
        cont.delete_all_objects(async=async)
        cont.object_manager.delete_all_objects.assert_called_once_with(
                [name1, name2], async=async)

    def test_cont_copy_object(self):
        cont = self.container
        cont.manager.copy_object = Mock()
        obj = utils.random_unicode()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        cont.copy_object(obj, new_container, new_obj_name=new_obj_name,
                content_type=content_type)
        cont.manager.copy_object.assert_called_once_with(cont, obj,
                new_container, new_obj_name=new_obj_name,
                content_type=content_type)

    def test_cont_move_object(self):
        cont = self.container
        cont.manager.move_object = Mock()
        obj = utils.random_unicode()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        new_reference = utils.random_unicode()
        content_type = utils.random_unicode()
        extra_info = utils.random_unicode()
        cont.move_object(obj, new_container, new_obj_name=new_obj_name,
                new_reference=new_reference, content_type=content_type,
                extra_info=extra_info)
        cont.manager.move_object.assert_called_once_with(cont, obj,
                new_container, new_obj_name=new_obj_name,
                new_reference=new_reference, content_type=content_type)

    def test_cont_change_object_content_type(self):
        cont = self.container
        cont.manager.change_object_content_type = Mock()
        obj = utils.random_unicode()
        new_ctype = utils.random_unicode()
        guess = utils.random_unicode()
        cont.change_object_content_type(obj, new_ctype, guess=guess)
        cont.manager.change_object_content_type.assert_called_once_with(cont,
                obj, new_ctype, guess=guess)

    def test_cont_get_temp_url(self):
        cont = self.container
        cont.manager.get_temp_url = Mock()
        obj = utils.random_unicode()
        seconds = utils.random_unicode()
        method = utils.random_unicode()
        key = utils.random_unicode()
        cont.get_temp_url(obj, seconds, method=method, key=key)
        cont.manager.get_temp_url.assert_called_once_with(cont, obj, seconds,
                method=method, key=key)

    def test_cont_get_object_metadata(self):
        cont = self.container
        cont.object_manager.get_metadata = Mock()
        obj = utils.random_unicode()
        cont.get_object_metadata(obj)
        cont.object_manager.get_metadata.assert_called_once_with(obj)

    def test_cont_set_object_metadata(self):
        cont = self.container
        cont.object_manager.set_metadata = Mock()
        obj = utils.random_unicode()
        meta_key = utils.random_unicode()
        meta_val = utils.random_unicode()
        metadata = {meta_key: meta_val}
        clear = utils.random_unicode()
        extra_info = utils.random_unicode()
        prefix = utils.random_unicode()
        cont.set_object_metadata(obj, metadata, clear=clear,
                extra_info=extra_info, prefix=prefix)
        cont.object_manager.set_metadata.assert_called_once_with(obj, metadata,
                clear=clear, prefix=prefix)

    def test_cont_list_subdirs(self):
        cont = self.container
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        full_listing = False
        cont.manager.list_subdirs = Mock()
        cont.list_subdirs(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, full_listing=full_listing)
        cont.manager.list_subdirs.assert_called_once_with(cont, marker=marker,
                limit=limit, prefix=prefix, delimiter=delimiter,
                full_listing=full_listing)

    def test_cont_remove_from_cache(self):
        obj = utils.random_unicode()
        self.assertIsNone(self.container.remove_from_cache(obj))

    def test_cont_cdn_props(self):
        for prop in ("cdn_enabled", "cdn_log_retention", "cdn_uri", "cdn_ttl",
                "cdn_ssl_uri", "cdn_streaming_uri", "cdn_ios_uri"):
            # Need a fresh container for each
            cont = self.client.create("fake")
            cont.manager.set_cdn_log_retention = Mock()
            val = getattr(cont, prop)
            self.assertTrue(val is not FAULT)
            newval = utils.random_unicode()
            setattr(cont, prop, newval)
            self.assertEqual(getattr(cont, prop), newval)

    def test_cmgr_list(self):
        cont = self.container
        mgr = cont.manager
        uri = utils.random_unicode()
        name1 = utils.random_unicode()
        name2 = utils.random_unicode()
        resp_body = [{"name": name1}, {"name": name2}]
        mgr.api.method_get = Mock(return_value=(None, resp_body))
        ret = mgr._list(uri)
        mgr.api.method_get.assert_called_once_with(uri)
        self.assertEqual(len(ret), 2)
        self.assertTrue(isinstance(ret[0], Container))

    def test_cmgr_get(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        cbytes = random.randint(1, 1000)
        ccount = random.randint(1, 1000)
        resp.headers = {"x-container-bytes-used": cbytes,
                "x-container-object-count": ccount}
        mgr.api.method_head = Mock(return_value=(resp, None))
        name = utils.random_unicode()
        ret = mgr.get(name)
        self.assertTrue(isinstance(ret, Container))
        self.assertEqual(ret.name, name)
        self.assertEqual(ret.total_bytes, cbytes)
        self.assertEqual(ret.object_count, ccount)

    def test_cmgr_get_not_found(self):
        cont = self.container
        mgr = cont.manager
        mgr.api.method_head = Mock(side_effect=exc.NotFound(""))
        name = utils.random_unicode()
        self.assertRaises(exc.NotFound, mgr.get, name)

    def test_cmgr_create(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.status_code = 201
        mgr.api.method_put = Mock(return_value=(resp, None))
        head_resp = fakes.FakeResponse()
        cbytes = random.randint(1, 1000)
        ccount = random.randint(1, 1000)
        head_resp.headers = {"x-container-bytes-used": cbytes,
                "x-container-object-count": ccount}
        mgr.api.method_head = Mock(return_value=(head_resp, None))
        name = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        prefix = utils.random_unicode()
        ret = mgr.create(name, metadata=metadata, prefix=prefix)
        exp_uri = "/%s" % name
        exp_headers = _massage_metakeys(metadata, prefix)
        mgr.api.method_put.assert_called_once_with(exp_uri, headers=exp_headers)
        mgr.api.method_head.assert_called_once_with(exp_uri)

    def test_cmgr_create_no_prefix(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.status_code = 201
        mgr.api.method_put = Mock(return_value=(resp, None))
        head_resp = fakes.FakeResponse()
        cbytes = random.randint(1, 1000)
        ccount = random.randint(1, 1000)
        head_resp.headers = {"x-container-bytes-used": cbytes,
                "x-container-object-count": ccount}
        mgr.api.method_head = Mock(return_value=(head_resp, None))
        name = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        prefix = None
        ret = mgr.create(name, metadata=metadata, prefix=prefix)
        exp_uri = "/%s" % name
        exp_headers = _massage_metakeys(metadata, CONTAINER_META_PREFIX)
        mgr.api.method_put.assert_called_once_with(exp_uri, headers=exp_headers)
        mgr.api.method_head.assert_called_once_with(exp_uri)

    def test_cmgr_create_fail(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.status_code = 400
        mgr.api.method_put = Mock(return_value=(resp, None))
        name = utils.random_unicode()
        self.assertRaises(exc.ClientException, mgr.create, name)

    def test_cmgr_delete(self):
        cont = self.container
        mgr = cont.manager
        names = utils.random_unicode()
        mgr.list_object_names = Mock(return_value=names)
        mgr.api.bulk_delete = Mock()
        exp_uri = "/%s" % cont.name
        mgr.api.method_delete = Mock(return_value=(None, None))
        mgr.delete(cont, del_objects=True)
        mgr.list_object_names.assert_called_once_with(cont)
        mgr.api.bulk_delete.assert_called_once_with(cont, names, async=False)
        mgr.api.method_delete.assert_called_once_with(exp_uri)

    def test_cmgr_create_body(self):
        cont = self.container
        mgr = cont.manager
        name = utils.random_unicode()
        ret = mgr._create_body(name)
        self.assertIsNone(ret)

    def test_cmgr_fetch_cdn_data(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.headers = utils.random_unicode()
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        ret = mgr.fetch_cdn_data(cont)
        exp_uri = "/%s" % cont.name
        mgr.api.cdn_request.assert_called_once_with(exp_uri, "HEAD")
        self.assertEqual(ret, resp.headers)

    def test_cmgr_fetch_cdn_data_not_cdn_enabled(self):
        cont = self.container
        mgr = cont.manager
        mgr.api.cdn_request = Mock(side_effect=exc.NotCDNEnabled(""))
        ret = mgr.fetch_cdn_data(cont)
        self.assertEqual(ret, {})

    def test_cmgr_get_account_headers(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.headers = utils.random_unicode()
        mgr.api.method_head = Mock(return_value=(resp, None))
        ret = mgr.get_account_headers()
        self.assertEqual(ret, resp.headers)
        mgr.api.method_head.assert_called_once_with("/")

    def test_cmgr_get_headers(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        resp.headers = utils.random_unicode()
        mgr.api.method_head = Mock(return_value=(resp, None))
        ret = mgr.get_headers(cont)
        exp_uri = "/%s" % cont.name
        self.assertEqual(ret, resp.headers)
        mgr.api.method_head.assert_called_once_with(exp_uri)

    def test_cmgr_get_account_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = utils.random_ascii()
        key_good = prefix + utils.random_ascii()
        key_bad = utils.random_ascii()
        val_good = utils.random_ascii()
        val_bad = utils.random_ascii()
        headers = {key_good: val_good, key_bad: val_bad}
        mgr.get_account_headers = Mock(return_value=headers)
        ret = mgr.get_account_metadata(prefix=prefix)
        self.assertEqual(ret, {key_good: val_good})

    def test_cmgr_get_account_metadata_no_prefix(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key_good_base = utils.random_ascii()
        key_good = ACCOUNT_META_PREFIX.lower() + key_good_base
        key_bad = utils.random_ascii()
        val_good = utils.random_ascii()
        val_bad = utils.random_ascii()
        headers = {key_good: val_good, key_bad: val_bad}
        mgr.get_account_headers = Mock(return_value=headers)
        ret = mgr.get_account_metadata(prefix=prefix)
        self.assertEqual(ret, {key_good_base: val_good})

    def test_cmgr_set_account_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        resp = fakes.FakeResponse()
        mgr.api.method_post = Mock(return_value=(resp, None))
        resp.status_code = 200
        ret = mgr.set_account_metadata(metadata, clear=False, prefix=prefix)
        self.assertTrue(ret)
        resp.status_code = 400
        ret = mgr.set_account_metadata(metadata, clear=False, prefix=prefix)
        self.assertFalse(ret)

    def test_cmgr_set_account_metadata_no_prefix(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        resp = fakes.FakeResponse()
        mgr.api.method_post = Mock(return_value=(resp, None))
        resp.status_code = 200
        ret = mgr.set_account_metadata(metadata, clear=False, prefix=prefix)
        self.assertTrue(ret)
        resp.status_code = 400
        ret = mgr.set_account_metadata(metadata, clear=False, prefix=prefix)

    def test_cmgr_set_account_metadata_clear(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        resp = fakes.FakeResponse()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        old_key = utils.random_unicode()
        old_val = utils.random_unicode()
        old_metadata = {old_key: old_val}
        mgr.api.method_post = Mock(return_value=(resp, None))
        mgr.get_account_metadata = Mock(return_value=old_metadata)
        resp.status_code = 200
        ret = mgr.set_account_metadata(metadata, clear=True, prefix=prefix)
        self.assertTrue(ret)

    def test_cmgr_delete_account_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        mgr.get_account_metadata = Mock(return_value=metadata)
        resp = fakes.FakeResponse()
        mgr.api.method_post = Mock(return_value=(resp, None))
        resp.status_code = 200
        ret = mgr.delete_account_metadata(prefix=prefix)
        self.assertTrue(ret)
        resp.status_code = 400
        ret = mgr.delete_account_metadata(prefix=prefix)
        self.assertFalse(ret)

    def test_cmgr_get_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = utils.random_ascii()
        key_good = prefix + utils.random_ascii()
        key_bad = utils.random_ascii()
        val_good = utils.random_ascii()
        val_bad = utils.random_ascii()
        headers = {key_good: val_good, key_bad: val_bad}
        mgr.get_headers = Mock(return_value=headers)
        ret = mgr.get_metadata(cont, prefix=prefix)
        self.assertEqual(ret, {key_good: val_good})

    def test_cmgr_get_metadata_no_prefix(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key_good_base = utils.random_ascii()
        key_good = CONTAINER_META_PREFIX.lower() + key_good_base
        key_bad = utils.random_ascii()
        val_good = utils.random_ascii()
        val_bad = utils.random_ascii()
        headers = {key_good: val_good, key_bad: val_bad}
        mgr.get_headers = Mock(return_value=headers)
        ret = mgr.get_metadata(cont, prefix=prefix)
        self.assertEqual(ret, {key_good_base: val_good})

    def test_cmgr_set_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key = utils.random_ascii()
        val = utils.random_ascii()
        metadata = {key: val}
        resp = fakes.FakeResponse()
        mgr.api.method_post = Mock(return_value=(resp, None))
        resp.status_code = 200
        ret = mgr.set_metadata(cont, metadata, clear=False, prefix=prefix)
        self.assertTrue(ret)
        resp.status_code = 400
        ret = mgr.set_metadata(cont, metadata, clear=False, prefix=prefix)

    def test_cmgr_set_metadata_clear(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        resp = fakes.FakeResponse()
        key = utils.random_ascii()
        val = utils.random_ascii()
        metadata = {key: val}
        old_key = utils.random_ascii()
        old_val = utils.random_ascii()
        old_metadata = {old_key: old_val}
        mgr.api.method_post = Mock(return_value=(resp, None))
        mgr.get_metadata = Mock(return_value=old_metadata)
        resp.status_code = 200
        ret = mgr.set_metadata(cont, metadata, clear=True, prefix=prefix)
        self.assertTrue(ret)

    def test_cmgr_remove_metadata_key(self):
        cont = self.container
        mgr = cont.manager
        key = utils.random_ascii()
        mgr.set_metadata = Mock()
        mgr.remove_metadata_key(cont, key)
        mgr.set_metadata.assert_called_once_with(cont, {key: ""})

    def test_cmgr_delete_metadata(self):
        cont = self.container
        mgr = cont.manager
        prefix = None
        key = utils.random_ascii()
        val = utils.random_ascii()
        metadata = {key: val}
        mgr.get_metadata = Mock(return_value=metadata)
        resp = fakes.FakeResponse()
        mgr.api.method_post = Mock(return_value=(resp, None))
        resp.status_code = 200
        ret = mgr.delete_metadata(cont, prefix=prefix)
        self.assertTrue(ret)

    def test_cmgr_get_cdn_metadata(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        key = utils.random_ascii()
        val = utils.random_ascii()
        headers = {key: val, "date": time.ctime()}
        resp.headers = headers
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        ret = mgr.get_cdn_metadata(cont)
        self.assertTrue(key in ret)
        self.assertFalse("date" in ret)

    def test_cmgr_set_cdn_metadata(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        key = "x-ttl"
        val = 666
        metadata = {key: val}
        exp_meta = {key: str(val)}
        exp_uri = "/%s" % cont.name
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        ret = mgr.set_cdn_metadata(cont, metadata)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, "POST",
                headers=exp_meta)

    def test_cmgr_set_cdn_metadata_invalid(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        key = "INVALID"
        val = 666
        metadata = {key: val}
        self.assertRaises(exc.InvalidCDNMetadata, mgr.set_cdn_metadata, cont,
                metadata)

    def test_cmgr_get_temp_url_no_key(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        seconds = utils.random_unicode()
        key = None
        mgr.api.get_temp_url_key = Mock(return_value=None)
        self.assertRaises(exc.MissingTemporaryURLKey, mgr.get_temp_url, cont,
                obj, seconds, key=key)

    def test_cmgr_get_temp_url_bad_method(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        seconds = utils.random_unicode()
        key = utils.random_unicode()
        method = "INVALID"
        self.assertRaises(exc.InvalidTemporaryURLMethod, mgr.get_temp_url, cont,
                obj, seconds, method=method, key=key)

    def test_cmgr_get_temp_url_unicode_error(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        seconds = random.randint(1, 1000)
        key = utils.random_unicode()
        method = "GET"
        mgr.api.management_url = "%s/v2/" % fakes.example_uri
        self.assertRaises(exc.UnicodePathError, mgr.get_temp_url, cont,
                obj, seconds, method=method, key=key)

    def test_cmgr_get_temp_url(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_ascii()
        seconds = random.randint(1, 1000)
        key = utils.random_ascii()
        method = "GET"
        mgmt_url = "%s/v2/" % fakes.example_uri
        mgr.api.management_url = mgmt_url
        ret = mgr.get_temp_url(cont, obj, seconds, method=method, key=key)
        self.assertTrue(ret.startswith(mgmt_url))
        self.assertTrue("temp_url_sig" in ret)
        self.assertTrue("temp_url_expires" in ret)

    def test_cmgr_list_containers_info(self):
        cont = self.container
        mgr = cont.manager
        limit = utils.random_unicode()
        marker = utils.random_unicode()
        body = utils.random_unicode()
        mgr.api.method_get = Mock(return_value=(None, body))
        ret = mgr.list_containers_info(limit=limit, marker=marker)
        self.assertEqual(mgr.api.method_get.call_count, 1)
        self.assertEqual(ret, body)

    def test_cmgr_list_public_containers(self):
        cont = self.container
        mgr = cont.manager
        name1 = utils.random_unicode()
        name2 = utils.random_unicode()
        body = [{"name": name1}, {"name": name2}]
        mgr.api.cdn_request = Mock(return_value=(None, body))
        ret = mgr.list_public_containers()
        mgr.api.cdn_request.assert_called_once_with("", "GET")
        self.assertTrue(isinstance(ret, list))
        self.assertTrue(name1 in ret)
        self.assertTrue(name2 in ret)

    def test_cmgr_make_public(self):
        cont = self.container
        mgr = cont.manager
        ttl = utils.random_unicode()
        mgr._set_cdn_access = Mock()
        mgr.make_public(cont, ttl=ttl)
        mgr._set_cdn_access.assert_called_once_with(cont, public=True, ttl=ttl)

    def test_cmgr_make_private(self):
        cont = self.container
        mgr = cont.manager
        mgr._set_cdn_access = Mock()
        mgr.make_private(cont)
        mgr._set_cdn_access.assert_called_once_with(cont, public=False)

    def test_cmgr_set_cdn_access(self):
        cont = self.container
        mgr = cont.manager
        for pub in (True, False):
            ttl = utils.random_unicode()
            exp_headers = {"X-Cdn-Enabled": str(pub)}
            if pub:
                exp_headers["X-Ttl"] = ttl
            exp_uri = "/%s" % cont.name
            mgr.api.cdn_request = Mock()
            mgr._set_cdn_access(cont, pub, ttl=ttl)
            mgr.api.cdn_request.assert_called_once_with(exp_uri, method="PUT",
                    headers=exp_headers)

    def test_cmgr_get_cdn_log_retention(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        retain = random.choice((True, False))
        headers = {"fake": "fake", "x-log-retention": str(retain)}
        resp.headers = headers
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        exp_uri = "/%s" % cont.name
        ret = mgr.get_cdn_log_retention(cont)
        self.assertEqual(ret, retain)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="HEAD")

    def test_cmgr_set_cdn_log_retention(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        retain = random.choice((True, False))
        exp_headers = {"X-Log-Retention": str(retain)}
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        exp_uri = "/%s" % cont.name
        mgr.set_cdn_log_retention(cont, retain)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="PUT",
                headers=exp_headers)

    def test_cmgr_get_container_streaming_uri(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        uri = utils.random_unicode()
        headers = {"fake": "fake", "x-cdn-streaming-uri": uri}
        resp.headers = headers
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        exp_uri = "/%s" % cont.name
        ret = mgr.get_container_streaming_uri(cont)
        self.assertEqual(ret, uri)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="HEAD")

    def test_cmgr_get_container_ios_uri(self):
        cont = self.container
        mgr = cont.manager
        resp = fakes.FakeResponse()
        uri = utils.random_unicode()
        headers = {"fake": "fake", "x-cdn-ios-uri": uri}
        resp.headers = headers
        mgr.api.cdn_request = Mock(return_value=(resp, None))
        exp_uri = "/%s" % cont.name
        ret = mgr.get_container_ios_uri(cont)
        self.assertEqual(ret, uri)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="HEAD")

    def test_cmgr_set_web_index_page(self):
        cont = self.container
        mgr = cont.manager
        page = utils.random_unicode()
        exp_headers = {"X-Container-Meta-Web-Index": page}
        exp_uri = "/%s" % cont.name
        mgr.api.cdn_request = Mock()
        mgr.set_web_index_page(cont, page)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="POST",
                headers=exp_headers)

    def test_cmgr_set_web_error_page(self):
        cont = self.container
        mgr = cont.manager
        page = utils.random_unicode()
        exp_headers = {"X-Container-Meta-Web-Error": page}
        exp_uri = "/%s" % cont.name
        mgr.api.cdn_request = Mock()
        mgr.set_web_error_page(cont, page)
        mgr.api.cdn_request.assert_called_once_with(exp_uri, method="POST",
                headers=exp_headers)

    @patch("pyrax.object_storage.assure_container")
    def test_cmgr_purge_cdn_object(self, mock_ac):
        cont = self.container
        mgr = cont.manager
        mock_ac.return_value = cont
        cont.purge_cdn_object = Mock()
        obj = utils.random_unicode()
        email_addresses = utils.random_unicode()
        mgr.purge_cdn_object(cont, obj, email_addresses=email_addresses)
        cont.purge_cdn_object.assert_called_once_with(obj,
                email_addresses=email_addresses)

    def test_cmgr_list_objects(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        cont.list = Mock()
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = False
        mgr.list_objects(cont, marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                full_listing=full_listing)
        cont.list.assert_called_once_with(marker=marker, limit=limit,
                prefix=prefix, delimiter=delimiter, end_marker=end_marker)

    def test_cmgr_list_objects_full(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        cont.list_all = Mock()
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = True
        mgr.list_objects(cont, marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                full_listing=full_listing)
        cont.list_all.assert_called_once_with(prefix=prefix)

    def test_cmgr_list_object_names(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        cont.list_object_names = Mock()
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = True
        mgr.list_object_names(cont, marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                full_listing=full_listing)
        cont.list_object_names.assert_called_once_with(marker=marker,
                limit=limit, prefix=prefix, delimiter=delimiter,
                end_marker=end_marker, full_listing=full_listing)

    @patch("pyrax.object_storage.StorageObjectIterator", new=fakes.FakeIterator)
    def test_cmgr_object_listing_iterator(self):
        cont = self.container
        mgr = cont.manager
        prefix = utils.random_unicode()
        ret = mgr.object_listing_iterator(cont, prefix=prefix)
        self.assertTrue(isinstance(ret, utils.ResultsIterator))

    def test_cmgr_list_subdirs(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        name1 = utils.random_ascii()
        name2 = utils.random_ascii()
        sdir = utils.random_ascii()
        objs = [{"name": name1, "content_type": "fake"},
                {"name": name2, "subdir": sdir, "content_type": "fake"}]
        cont.list = Mock(return_value=objs)
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        full_listing = False
        ret = mgr.list_subdirs(cont, marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, full_listing=full_listing)
        cont.list.assert_called_once_with(marker=marker, limit=limit,
                prefix=prefix, delimiter="/", return_raw=True)
        self.assertEqual(ret[0].name, sdir)

    def test_cmgr_get_object(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        cont.get = Mock()
        obj = utils.random_unicode()
        mgr.get_object(cont, obj)
        cont.get.assert_called_once_with(obj)

    def test_cmgr_create_object(self):
        cont = self.container
        mgr = cont.manager
        cont.create = Mock()
        file_or_path = utils.random_unicode()
        data = utils.random_unicode()
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        chunk_size = utils.random_unicode()
        headers = utils.random_unicode()
        return_none = utils.random_unicode()
        mgr.create_object(cont, file_or_path=file_or_path, data=data,
                obj_name=obj_name, content_type=content_type, etag=etag,
                content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata, chunk_size=chunk_size, headers=headers,
                return_none=return_none)
        cont.create.assert_called_once_with(file_or_path=file_or_path,
                data=data, obj_name=obj_name, content_type=content_type,
                etag=etag, content_encoding=content_encoding,
                content_length=content_length, ttl=ttl, chunked=chunked,
                metadata=metadata, chunk_size=chunk_size, headers=headers,
                return_none=return_none)

    def test_cmgr_fetch_object(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        include_meta = utils.random_unicode()
        chunk_size = utils.random_unicode()
        size = utils.random_unicode()
        extra_info = utils.random_unicode()
        cont.fetch = Mock()
        mgr.fetch_object(cont, obj, include_meta=include_meta,
                chunk_size=chunk_size, size=size, extra_info=extra_info)
        cont.fetch.assert_called_once_with(obj, include_meta=include_meta,
                chunk_size=chunk_size, size=size)

    def test_cmgr_fetch_partial(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        size = utils.random_unicode()
        cont.fetch_partial = Mock()
        mgr.fetch_partial(cont, obj, size)
        cont.fetch_partial.assert_called_once_with(obj, size)

    def test_cmgr_download_object(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        directory = utils.random_unicode()
        structure = utils.random_unicode()
        cont.download = Mock()
        mgr.download_object(cont, obj, directory, structure=structure)
        cont.download.assert_called_once_with(obj, directory,
                structure=structure)

    def test_cmgr_delete_object(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        cont.delete_object = Mock()
        mgr.delete_object(cont, obj)
        cont.delete_object.assert_called_once_with(obj)

    def test_cmgr_copy_object(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        resp = fakes.FakeResponse()
        etag = utils.random_unicode()
        resp.headers = {"etag": etag}
        mgr.api.method_put = Mock(return_value=(resp, None))
        exp_uri = "/%s/%s" % (new_container, new_obj_name)
        exp_from = "/%s/%s" % (cont.name, obj)
        exp_headers = {"X-Copy-From": exp_from,
                "Content-Length": "0",
                "Content-Type": content_type}
        ret = mgr.copy_object(cont, obj, new_container,
                new_obj_name=new_obj_name, content_type=content_type)
        mgr.api.method_put.assert_called_once_with(exp_uri, headers=exp_headers)
        self.assertEqual(ret, etag)

    def test_cmgr_move_object(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        new_obj = utils.random_unicode()
        mgr.copy_object = Mock(return_value=etag)
        mgr.delete_object = Mock()
        mgr.get_object = Mock(return_value=new_obj)
        for new_reference in (True, False):
            ret = mgr.move_object(cont, obj, new_container,
                    new_obj_name=new_obj_name, new_reference=new_reference,
                    content_type=content_type)
            if new_reference:
                self.assertEqual(ret, new_obj)
            else:
                self.assertEqual(ret, etag)

    def test_cmgr_move_object_fail(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        new_obj = utils.random_unicode()
        mgr.copy_object = Mock(return_value=None)
        mgr.delete_object = Mock()
        mgr.get_object = Mock()
        new_reference = False
        ret = mgr.move_object(cont, obj, new_container,
                new_obj_name=new_obj_name, new_reference=new_reference,
                content_type=content_type)
        self.assertIsNone(ret)

    @patch("mimetypes.guess_type")
    def test_cmgr_change_object_content_type(self, mock_guess):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        obj = utils.random_unicode()
        new_ctype = utils.random_unicode()
        cont.cdn_enabled = True
        cont.cdn_uri = utils.random_unicode()
        for guess in (True, False):
            if guess:
                mock_guess.return_value = (new_ctype, None)
            mgr.copy_object = Mock()
            mgr.change_object_content_type(cont, obj, new_ctype, guess=guess)
            mgr.copy_object.assert_called_once_with(cont, obj, cont,
                    content_type=new_ctype)

    def test_cmgr_delete_object_in_seconds(self):
        cont = self.container
        mgr = cont.manager
        obj = utils.random_unicode()
        seconds = utils.random_unicode()
        extra_info = utils.random_unicode()
        mgr.set_object_metadata = Mock()
        exp_meta = {"X-Delete-After": seconds}
        mgr.delete_object_in_seconds(cont, obj, seconds, extra_info=extra_info)
        mgr.set_object_metadata.assert_called_once_with(cont, obj, exp_meta,
                clear=True, prefix="")

    def test_cmgr_get_object_metadata(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        obj = utils.random_unicode()
        cont.get_object_metadata = Mock()
        mgr.get_object_metadata(cont, obj)
        cont.get_object_metadata.assert_called_once_with(obj)

    def test_cmgr_set_object_metadata(self):
        cont = self.container
        mgr = cont.manager
        mgr.get = Mock(return_value=cont)
        obj = utils.random_unicode()
        metadata = utils.random_unicode()
        clear = random.choice((True, False))
        prefix = utils.random_unicode()
        cont.set_object_metadata = Mock()
        mgr.set_object_metadata(cont, obj, metadata, clear=clear,
                prefix=prefix)
        cont.set_object_metadata.assert_called_once_with(obj, metadata,
                clear=clear, prefix=prefix)

    def test_sobj_repr(self):
        obj = self.obj
        obj_repr = "%s" % obj
        self.assertTrue("<Object " in obj_repr)
        self.assertTrue(obj.name in obj_repr)

    def test_sobj_id(self):
        cont = self.container
        nm = utils.random_unicode()
        obj = StorageObject(cont.object_manager, {"name": nm})
        self.assertEqual(obj.name, nm)
        self.assertEqual(obj.id, nm)

    def test_sobj_total_bytes(self):
        obj = self.obj
        num_bytes = random.randint(1, 100000)
        obj.bytes = num_bytes
        self.assertEqual(obj.total_bytes, num_bytes)

    def test_sobj_etag(self):
        obj = self.obj
        hashval = utils.random_unicode()
        obj.hash = hashval
        self.assertEqual(obj.etag, hashval)

    def test_sobj_container(self):
        obj = self.obj
        fake_cont = utils.random_unicode()
        obj.manager._container = fake_cont
        cont = obj.container
        self.assertEqual(cont, fake_cont)

    def test_sobj_get(self):
        obj = self.obj
        mgr = obj.manager
        mgr.fetch = Mock()
        include_meta = utils.random_unicode()
        chunk_size = utils.random_unicode()
        obj.get(include_meta=include_meta, chunk_size=chunk_size)
        mgr.fetch.assert_called_once_with(obj=obj, include_meta=include_meta,
                chunk_size=chunk_size)

    def test_sobj_fetch(self):
        obj = self.obj
        mgr = obj.manager
        mgr.fetch = Mock()
        include_meta = utils.random_unicode()
        chunk_size = utils.random_unicode()
        obj.fetch(include_meta=include_meta, chunk_size=chunk_size)
        mgr.fetch.assert_called_once_with(obj=obj, include_meta=include_meta,
                chunk_size=chunk_size)

    def test_sobj_download(self):
        obj = self.obj
        mgr = obj.manager
        mgr.download = Mock()
        directory = utils.random_unicode()
        structure = utils.random_unicode()
        obj.download(directory, structure=structure)
        mgr.download.assert_called_once_with(obj, directory,
                structure=structure)

    def test_sobj_copy(self):
        obj = self.obj
        mgr = obj.manager
        cont = obj.container
        cont.copy_object = Mock()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        extra_info = utils.random_unicode()
        obj.copy(new_container, new_obj_name=new_obj_name,
                extra_info=extra_info)
        cont.copy_object.assert_called_once_with(obj, new_container,
                new_obj_name=new_obj_name)

    def test_sobj_move(self):
        obj = self.obj
        mgr = obj.manager
        cont = obj.container
        cont.move_object = Mock()
        new_container = utils.random_unicode()
        new_obj_name = utils.random_unicode()
        extra_info = utils.random_unicode()
        obj.move(new_container, new_obj_name=new_obj_name,
                extra_info=extra_info)
        cont.move_object.assert_called_once_with(obj, new_container,
                new_obj_name=new_obj_name)

    def test_sobj_change_content_type(self):
        obj = self.obj
        mgr = obj.manager
        cont = obj.container
        cont.change_object_content_type = Mock()
        new_ctype = utils.random_unicode()
        guess = utils.random_unicode()
        obj.change_content_type(new_ctype, guess=guess)
        cont.change_object_content_type.assert_called_once_with(obj,
                new_ctype=new_ctype, guess=guess)

    def test_sobj_purge(self):
        obj = self.obj
        mgr = obj.manager
        email_addresses = utils.random_unicode()
        mgr.purge = Mock()
        obj.purge(email_addresses=email_addresses)
        mgr.purge.assert_called_once_with(obj, email_addresses=email_addresses)

    def test_sobj_get_metadata(self):
        obj = self.obj
        mgr = obj.manager
        mgr.get_metadata = Mock()
        obj.get_metadata()
        mgr.get_metadata.assert_called_once_with(obj)

    def test_sobj_set_metadata(self):
        obj = self.obj
        mgr = obj.manager
        mgr.set_metadata = Mock()
        metadata = utils.random_unicode()
        clear = utils.random_unicode()
        prefix = utils.random_unicode()
        obj.set_metadata(metadata, clear=clear, prefix=prefix)
        mgr.set_metadata.assert_called_once_with(obj, metadata, clear=clear,
                prefix=prefix)

    def test_sobj_remove_metadata_key(self):
        obj = self.obj
        mgr = obj.manager
        mgr.remove_metadata_key = Mock()
        key = utils.random_unicode()
        prefix = utils.random_unicode()
        obj.remove_metadata_key(key, prefix=prefix)
        mgr.remove_metadata_key.assert_called_once_with(obj, key, prefix=prefix)

    def test_sobj_get_temp_url(self):
        obj = self.obj
        cont = obj.container
        cont.get_temp_url = Mock()
        seconds = utils.random_unicode()
        method = utils.random_unicode()
        obj.get_temp_url(seconds, method=method)
        cont.get_temp_url.assert_called_once_with(obj, seconds=seconds,
                method=method)

    def test_sobj_delete_in_seconds(self):
        obj = self.obj
        cont = obj.container
        cont.delete_object_in_seconds = Mock()
        seconds = utils.random_unicode()
        obj.delete_in_seconds(seconds)
        cont.delete_object_in_seconds.assert_called_once_with(obj, seconds)

    def test_sobj_iter_init_methods(self):
        client = self.client
        mgr = client._manager
        it = StorageObjectIterator(mgr)
        self.assertEqual(it.list_method, mgr.list)

    def test_sobj_mgr_name(self):
        cont = self.container
        mgr = cont.object_manager
        self.assertEqual(mgr.name, mgr.uri_base)

    def test_sobj_mgr_container(self):
        cont = self.container
        mgr = cont.object_manager
        new_cont = utils.random_unicode()
        mgr._container = new_cont
        self.assertEqual(mgr.container, new_cont)

    def test_sobj_mgr_container_missing(self):
        cont = self.container
        mgr = cont.object_manager
        delattr(mgr, "_container")
        new_cont = utils.random_unicode()
        mgr.api.get = Mock(return_value=new_cont)
        self.assertEqual(mgr.container, new_cont)

    def test_sobj_mgr_list_raw(self):
        cont = self.container
        mgr = cont.object_manager
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = utils.random_unicode()
        fake_resp = utils.random_unicode()
        fake_resp_body = utils.random_unicode()
        mgr.api.method_get = Mock(return_value=(fake_resp, fake_resp_body))
        ret = mgr.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        self.assertEqual(ret, fake_resp_body)

    def test_sobj_mgr_list_obj(self):
        cont = self.container
        mgr = cont.object_manager
        marker = utils.random_unicode()
        limit = utils.random_unicode()
        prefix = utils.random_unicode()
        delimiter = utils.random_unicode()
        end_marker = utils.random_unicode()
        return_raw = False
        fake_resp = utils.random_unicode()
        nm = utils.random_unicode()
        fake_resp_body = [{"name": nm}]
        mgr.api.method_get = Mock(return_value=(fake_resp, fake_resp_body))
        ret = mgr.list(marker=marker, limit=limit, prefix=prefix,
                delimiter=delimiter, end_marker=end_marker,
                return_raw=return_raw)
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 1)
        obj = ret[0]
        self.assertEqual(obj.name, nm)

    def test_sobj_mgr_get(self):
        cont = self.container
        mgr = cont.object_manager
        obj = utils.random_unicode()
        contlen = random.randint(100, 1000)
        conttype = utils.random_unicode()
        etag = utils.random_unicode()
        lastmod = utils.random_unicode()
        fake_resp = fakes.FakeResponse()
        fake_resp.headers = {"content-length": contlen,
                "content-type": conttype,
                "etag": etag,
                "last-modified": lastmod,
                }
        mgr.api.method_head = Mock(return_value=(fake_resp, None))
        ret = mgr.get(obj)
        self.assertEqual(ret.name, obj)
        self.assertEqual(ret.bytes, contlen)
        self.assertEqual(ret.content_type, conttype)
        self.assertEqual(ret.hash, etag)
        self.assertEqual(ret.last_modified, lastmod)

    def test_sobj_mgr_get_no_length(self):
        cont = self.container
        mgr = cont.object_manager
        obj = utils.random_unicode()
        contlen = None
        conttype = utils.random_unicode()
        etag = utils.random_unicode()
        lastmod = utils.random_unicode()
        fake_resp = fakes.FakeResponse()
        fake_resp.headers = {"content-length": contlen,
                "content-type": conttype,
                "etag": etag,
                "last-modified": lastmod,
                }
        mgr.api.method_head = Mock(return_value=(fake_resp, None))
        ret = mgr.get(obj)
        self.assertEqual(ret.bytes, contlen)

    def test_sobj_mgr_create_empty(self):
        cont = self.container
        mgr = cont.object_manager
        self.assertRaises(exc.NoContentSpecified, mgr.create)

    def test_sobj_mgr_create_no_name(self):
        cont = self.container
        mgr = cont.object_manager
        self.assertRaises(exc.MissingName, mgr.create, data="x")

    def test_sobj_mgr_create_data(self):
        cont = self.container
        mgr = cont.object_manager
        data = utils.random_unicode()
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        headers = {"X-Delete-After": ttl}
        massaged = _massage_metakeys(metadata, OBJECT_META_PREFIX)
        headers.update(massaged)
        for return_none in (True, False):
            mgr._upload = Mock()
            get_resp = utils.random_unicode()
            mgr.get = Mock(return_value=get_resp)
            ret = mgr.create(data=data, obj_name=obj_name,
                    content_type=content_type, etag=etag,
                    content_encoding=content_encoding,
                    content_length=content_length, ttl=ttl, chunked=chunked,
                    metadata=metadata, chunk_size=chunk_size, headers=headers,
                    return_none=return_none)
            mgr._upload.assert_called_once_with(obj_name, data, content_type,
                    content_encoding, content_length, etag, bool(chunk_size),
                    chunk_size, headers)
            if return_none:
                self.assertIsNone(ret)
            else:
                self.assertEqual(ret, get_resp)

    def test_sobj_mgr_create_file(self):
        cont = self.container
        mgr = cont.object_manager
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        headers = {"X-Delete-After": ttl}
        massaged = _massage_metakeys(metadata, OBJECT_META_PREFIX)
        headers.update(massaged)
        for return_none in (True, False):
            mgr._upload = Mock()
            get_resp = utils.random_unicode()
            mgr.get = Mock(return_value=get_resp)
            with utils.SelfDeletingTempfile() as tmp:
                ret = mgr.create(tmp, obj_name=obj_name,
                        content_type=content_type, etag=etag,
                        content_encoding=content_encoding,
                        content_length=content_length, ttl=ttl,
                        chunked=chunked, metadata=metadata,
                        chunk_size=chunk_size, headers=headers,
                        return_none=return_none)
                self.assertEqual(mgr._upload.call_count, 1)
                call_args = list(mgr._upload.call_args)[0]
                for param in (obj_name, content_type, content_encoding,
                        content_length, etag, False, headers):
                    self.assertTrue(param in call_args)
            if return_none:
                self.assertIsNone(ret)
            else:
                self.assertEqual(ret, get_resp)

    def test_sobj_mgr_create_file_obj(self):
        cont = self.container
        mgr = cont.object_manager
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        etag = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        ttl = utils.random_unicode()
        chunked = utils.random_unicode()
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        metadata = {key: val}
        headers = {"X-Delete-After": ttl}
        massaged = _massage_metakeys(metadata, OBJECT_META_PREFIX)
        headers.update(massaged)
        for return_none in (True, False):
            mgr._upload = Mock()
            get_resp = utils.random_unicode()
            mgr.get = Mock(return_value=get_resp)
            with utils.SelfDeletingTempfile() as tmp:
                with open(tmp) as tmpfile:
                    ret = mgr.create(tmpfile, obj_name=obj_name,
                            content_type=content_type, etag=etag,
                            content_encoding=content_encoding,
                            content_length=content_length, ttl=ttl,
                            chunked=chunked, metadata=metadata,
                            chunk_size=chunk_size, headers=headers,
                            return_none=return_none)
                self.assertEqual(mgr._upload.call_count, 1)
                call_args = list(mgr._upload.call_args)[0]
                for param in (obj_name, content_type, content_encoding,
                        content_length, etag, False, headers):
                    self.assertTrue(param in call_args)
            if return_none:
                self.assertIsNone(ret)
            else:
                self.assertEqual(ret, get_resp)

    def test_sobj_mgr_upload(self):
        obj = self.obj
        mgr = obj.manager
        obj_name = utils.random_unicode()
        content = utils.random_unicode()
        content_type = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = utils.random_unicode()
        etag = utils.random_unicode()
        chunked = utils.random_unicode()
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        headers = {key: val}
        mgr._store_object = Mock()
        ret = mgr._upload(obj_name, content, content_type, content_encoding,
                content_length, etag, chunked, chunk_size, headers)
        mgr._store_object.assert_called_once_with(obj_name, content=content,
                etag=etag, chunked=chunked, chunk_size=chunk_size,
                headers=headers)

    def test_sobj_mgr_upload_file(self):
        obj = self.obj
        mgr = obj.manager
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = random.randint(10, 1000)
        etag = utils.random_unicode()
        chunked = utils.random_unicode()
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        headers = {key: val}
        mgr._store_object = Mock()
        with utils.SelfDeletingTempfile() as tmp:
            with open(tmp) as content:
                ret = mgr._upload(obj_name, content, content_type,
                        content_encoding, content_length, etag, chunked,
                        chunk_size, headers)
        mgr._store_object.assert_called_once_with(obj_name, content=content,
                etag=etag, chunked=chunked, chunk_size=chunk_size,
                headers=headers)

    def test_sobj_mgr_upload_file_unchunked(self):
        obj = self.obj
        mgr = obj.manager
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = random.randint(10, 1000)
        etag = utils.random_unicode()
        chunked = None
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        headers = {key: val}
        mgr._store_object = Mock()
        with utils.SelfDeletingTempfile() as tmp:
            with open(tmp) as content:
                ret = mgr._upload(obj_name, content, content_type,
                        content_encoding, content_length, etag, chunked,
                        chunk_size, headers)
        mgr._store_object.assert_called_once_with(obj_name, content=content,
                etag=etag, chunked=chunked, chunk_size=chunk_size,
                headers=headers)

    def test_sobj_mgr_upload_file_unchunked_no_length(self):
        obj = self.obj
        mgr = obj.manager
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = None
        etag = utils.random_unicode()
        chunked = None
        chunk_size = utils.random_unicode()
        key = utils.random_unicode()
        val = utils.random_unicode()
        headers = {key: val}
        mgr._store_object = Mock()
        with utils.SelfDeletingTempfile() as tmp:
            with open(tmp) as content:
                ret = mgr._upload(obj_name, content, content_type,
                        content_encoding, content_length, etag, chunked,
                        chunk_size, headers)
        mgr._store_object.assert_called_once_with(obj_name, content=content,
                etag=etag, chunked=chunked, chunk_size=chunk_size,
                headers=headers)

    def test_sobj_mgr_upload_multiple(self):
        obj = self.obj
        mgr = obj.manager
        sav = pyrax.object_storage.MAX_FILE_SIZE
        pyrax.object_storage.MAX_FILE_SIZE = 42
        obj_name = utils.random_unicode()
        content_type = utils.random_unicode()
        content_encoding = utils.random_unicode()
        content_length = None
        etag = utils.random_unicode()
        chunked = None
        chunk_size = None
        key = utils.random_unicode()
        val = utils.random_unicode()
        headers = {key: val}
        mgr._store_object = Mock()
        with utils.SelfDeletingTempfile() as tmp:
            with open(tmp, "w") as content:
                content.write("x" * 66)
            with open(tmp) as content:
                ret = mgr._upload(obj_name, content, content_type,
                        content_encoding, content_length, etag, chunked,
                        chunk_size, headers)
                self.assertEqual(mgr._store_object.call_count, 3)
        pyrax.object_storage.MAX_FILE_SIZE = sav

    def test_sobj_mgr_store_object(self):
        obj = self.obj
        mgr = obj.manager
        obj_name = utils.random_unicode()
        content = utils.random_unicode()
        etag = None
        chunk_size = utils.random_unicode()
        val = utils.random_unicode()
        headers = {"Content-Length": val}
        exp_uri = "/%s/%s" % (mgr.uri_base, obj_name)
        for chunked in (True, False):
            mgr.api.method_put = Mock(return_value=(None, None))
            exp_hdrs = {"Content-Length": val, "Content-Type": None}
            if chunked:
                exp_hdrs.pop("Content-Length")
                exp_hdrs["Transfer-Encoding"] = "chunked"
            else:
                exp_hdrs["ETag"] = utils.get_checksum(content)
            mgr._store_object(obj_name, content, etag=etag, chunked=chunked,
                    chunk_size=chunk_size, headers=headers)
            mgr.api.method_put.assert_called_once_with(exp_uri, data=content,
                    headers=headers)

    def test_sobj_mgr_fetch_no_chunk(self):
        obj = self.obj
        mgr = obj.manager
        chunk_size = None
        size = random.randint(1, 1000)
        extra_info = utils.random_unicode()
        resp = fakes.FakeResponse()
        resp_body = utils.random_unicode()
        exp_uri = "/%s/%s" % (mgr.uri_base, obj.name)
        exp_headers = {"Range": "bytes=0-%s" % size}
        for include_meta in (True, False):
            mgr.api.method_get = Mock(return_value=(resp, resp_body))
            ret = mgr.fetch(obj, include_meta=include_meta, chunk_size=chunk_size,
                    size=size, extra_info=extra_info)
            mgr.api.method_get.assert_called_once_with(exp_uri, headers=exp_headers)
            self.assertEqual(ret, resp_body)








if __name__ == "__main__":
    unittest.main()
