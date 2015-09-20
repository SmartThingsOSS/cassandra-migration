package smartthings.util

import spock.lang.Specification
import spock.lang.Unroll

class UtilSpec extends Specification {

	@Unroll
	def 'all is #expected for input #strings'() {
		expect:
		Util.all(*strings) == expected

		where:
		strings        || expected
		['foo', 'bar'] || true
		['foo', null]  || false
		['foo', '']    || false
	}
}
