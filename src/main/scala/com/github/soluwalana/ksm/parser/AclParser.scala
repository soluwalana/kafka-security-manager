package com.github.soluwalana.ksm.parser

import java.io.Reader

import com.github.soluwalana.ksm.source.SourceAclResult
import kafka.security.auth.{Acl, Resource}

trait AclParser {

  def aclsFromReader(reader: Reader): SourceAclResult

  def formatAcls(acls: List[(Resource, Acl)]): String

}
