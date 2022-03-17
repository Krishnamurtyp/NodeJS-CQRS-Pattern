const identityProjection = {
    $init(){
        return {
            id:null,
            email:null,
            isRegistered: false
        }
    },
    Registered(identity, registered){
        identity.id = registered.data.userId
        identity.email = registered.data.email
        identity.isregistered = true
        
        return identity
    }
}