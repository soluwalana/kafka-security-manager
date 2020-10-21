package com.github.soluwalana.ksm.grpc

import com.github.soluwalana.ksm.notification.DummyNotification
import com.github.soluwalana.ksm.parser.CsvAclParser
import com.github.soluwalana.ksm.source.DummySourceAcl
import com.github.soluwalana.ksm.{AclSynchronizer, DummyAuthorizer}
import com.security.kafka.pb.ksm.OperationTypePb._
import com.security.kafka.pb.ksm.PermissionTypePb._
import com.security.kafka.pb.ksm.ResourceTypePb._
import com.security.kafka.pb.ksm._
import org.scalatest.{AsyncFlatSpec, Matchers}

class KsmServiceImplTest extends AsyncFlatSpec with Matchers {

  val dummySourceAcl = new DummySourceAcl

  val ksmServiceImpl = new KsmServiceImpl(
    new AclSynchronizer(
      new DummyAuthorizer(),
      dummySourceAcl,
      new DummyNotification,
      new CsvAclParser
    )
  )

  "getAllAcls" should "return all Acls" in {
    ksmServiceImpl.getAllAcls(new GetAllAclsRequest) map { getAclResponse =>
      getAclResponse shouldBe GetAllAclsResponse(
        Vector(
          ResourceAndAclPb(
            Some(
              ResourcePb(
                "foo",
                RESOURCE_TYPE_TOPIC,
                PatternTypePb.PATTERN_TYPE_LITERAL
              )
            ),
            Some(
              AclPb(
                Some(KafkaPrincipalPb("User", "alice")),
                PERMISSION_TYPE_ALLOW,
                "*",
                OPERATION_TYPE_READ
              )
            )
          )
        )
      )

    }
  }
}
