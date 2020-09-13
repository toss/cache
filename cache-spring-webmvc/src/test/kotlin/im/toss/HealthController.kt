package im.toss

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/")
class HealthController {
    @GetMapping("/health")
    fun health(): String {
        return "OK"
    }
}
